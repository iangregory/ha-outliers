#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "mysql-connector-python>=8.0",
#     "rich>=13.0",
# ]
# ///
"""
Home Assistant MariaDB Outlier Detector

Connects to a Home Assistant MariaDB database and identifies extreme outlier
values in states history entries. Provides a simple text interface
to review and optionally edit/delete them.
"""

import json
import signal
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Any

import mysql.connector
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)
from rich.prompt import Confirm, Prompt
from rich.table import Table

# Configuration constants
SIGMA_THRESHOLD = 5.0  # Standard deviations from mean to flag as outlier
MIN_SAMPLES = 200  # Minimum samples required for statistical analysis
FREQUENCY_THRESHOLD = 0.01  # Exclude values appearing in >1% of samples
PAGE_SIZE = 25  # Number of outliers per page in display
CONNECTION_TIMEOUT = 10  # Database connection timeout in seconds

console = Console()
CONFIG_FILE = Path.home() / ".config" / "ha-outliers" / "config.json"
VALID_STATE_CONDITION = (
    "state NOT IN ('unavailable', 'unknown', '') AND state IS NOT NULL"
)


def handle_interrupt(signum: int, frame: FrameType | None) -> None:
    """Handle Ctrl-C gracefully."""
    console.print("\n[yellow]Interrupted.[/yellow]")
    sys.exit(130)


def format_number(n: float) -> str:
    """Format a number without scientific notation."""
    if abs(n) >= 1000:
        return f"{n:,.0f}"
    return f"{n:.4f}".rstrip("0").rstrip(".")


def get_db_connection(
    host: str = "localhost",
    port: int = 3306,
    user: str = "homeassistant",
    password: str = "",
    database: str = "homeassistant",
) -> mysql.connector.MySQLConnection:
    """Create a connection to the MariaDB database."""
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        connection_timeout=CONNECTION_TIMEOUT,
    )


def load_cached_config() -> dict | None:
    """Load cached configuration from file."""
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except (json.JSONDecodeError, IOError):
            pass
    return None


def save_cached_config(config: dict) -> None:
    """Save configuration to cache file."""
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(config, indent=2))
    CONFIG_FILE.chmod(0o600)


def find_outliers_in_states(
    conn: mysql.connector.MySQLConnection, min_samples: int = MIN_SAMPLES
) -> list[dict]:
    """Find extreme outliers in the states table using σ threshold."""
    numeric_prefixes = ("sensor.", "number.", "counter.", "input_number.")
    prefix_conditions = " OR ".join(
        f"sm.entity_id LIKE '{p}%'" for p in numeric_prefixes
    )

    with conn.cursor(dictionary=True) as cursor:
        # Get candidate entities with most recent state
        cursor.execute(f"""
            SELECT sm.metadata_id, sm.entity_id,
                   (SELECT s.state FROM states s WHERE s.metadata_id = sm.metadata_id
                      AND {VALID_STATE_CONDITION} ORDER BY s.last_updated_ts DESC LIMIT 1) as recent_state
            FROM states_meta sm WHERE {prefix_conditions}
        """)

        # Filter to numeric entities only
        candidates = []
        for c in cursor.fetchall():
            try:
                float(c["recent_state"])
                candidates.append(c)
            except (ValueError, TypeError):
                continue

        all_outliers: list[dict[str, Any]] = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Scanning {len(candidates)} entities...", total=len(candidates)
            )

            for candidate in candidates:
                meta_id, entity_id = candidate["metadata_id"], candidate["entity_id"]
                progress.update(
                    task, advance=1, description=f"[cyan]{entity_id[:50]}[/cyan]"
                )

                # Get statistics from DB
                cursor.execute(
                    f"""
                    SELECT COUNT(*) as cnt, AVG(CAST(state AS DECIMAL(30,10))) as mean_val,
                           STDDEV(CAST(state AS DECIMAL(30,10))) as std_val
                    FROM states WHERE metadata_id = %s AND {VALID_STATE_CONDITION}
                """,
                    (meta_id,),
                )
                stats = cursor.fetchone()

                if not stats or stats["cnt"] < min_samples or not stats["std_val"]:
                    continue

                total_samples, mean_val, std_val = (
                    stats["cnt"],
                    float(stats["mean_val"]),
                    float(stats["std_val"]),
                )
                if std_val == 0:
                    continue

                lower = mean_val - SIGMA_THRESHOLD * std_val
                upper = mean_val + SIGMA_THRESHOLD * std_val

                # Find outliers outside bounds
                cursor.execute(
                    f"""
                    SELECT state_id, CAST(state AS DECIMAL(30,10)) as numeric_value, last_updated_ts
                    FROM states WHERE metadata_id = %s AND {VALID_STATE_CONDITION}
                      AND (CAST(state AS DECIMAL(30,10)) < %s OR CAST(state AS DECIMAL(30,10)) > %s)
                """,
                    (meta_id, lower, upper),
                )

                outlier_rows = cursor.fetchall()
                if not outlier_rows:
                    continue

                value_counts = Counter(
                    f"{float(r['numeric_value']):.6g}" for r in outlier_rows
                )

                for row in outlier_rows:
                    value = float(row["numeric_value"])
                    occurrence_count = value_counts[f"{value:.6g}"]
                    if occurrence_count > total_samples * FREQUENCY_THRESHOLD:
                        continue

                    all_outliers.append(
                        {
                            "id": row["state_id"],
                            "metadata_id": meta_id,
                            "entity_id": entity_id,
                            "value": value,
                            "lower_bound": lower,
                            "upper_bound": upper,
                            "mean": mean_val,
                            "deviation": abs(value - mean_val) / std_val,
                            "timestamp": row["last_updated_ts"],
                            "total_samples": total_samples,
                        }
                    )

    return sorted(all_outliers, key=lambda x: x["deviation"], reverse=True)


def group_outliers(outliers: list[dict]) -> list[dict]:
    """Group outliers by entity_id, direction, and deviation band."""
    groups: dict[tuple, dict] = {}
    for o in outliers:
        # Use wider bands for extreme outliers: 1σ bands up to 10σ, 2σ bands up to 20σ, 5σ bands beyond
        dev = o["deviation"]
        band_size = 1 if dev < 10 else (2 if dev < 20 else 5)
        band = int(dev // band_size) * band_size
        direction = "above" if o["value"] > o["mean"] else "below"
        key = (o["entity_id"], band, direction)
        if key not in groups:
            groups[key] = {
                **o,
                "count": 1,
                "ids": [o["id"]],
                "timestamps": [o["timestamp"]],
                "values": [o["value"]],
                "min_value": o["value"],
                "max_value": o["value"],
            }
        else:
            g = groups[key]
            g["count"] += 1
            g["ids"].append(o["id"])
            g["timestamps"].append(o["timestamp"])
            g["values"].append(o["value"])
            g["min_value"], g["max_value"] = (
                min(g["min_value"], o["value"]),
                max(g["max_value"], o["value"]),
            )
    return sorted(groups.values(), key=lambda x: x["deviation"], reverse=True)


def format_timestamp(ts: float | None) -> str:
    """Convert Unix timestamp to readable datetime string."""
    if ts is None:
        return "N/A"
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError, OverflowError):
        return str(ts)


def display_outliers(
    outliers: list[dict[str, Any]], page_size: int = PAGE_SIZE, start_page: int = 0
) -> tuple[str, int, dict] | None:
    """Display grouped outliers in a paginated table. Returns (action, index, outlier) or None."""
    active = [o for o in outliers if not o.get("_removed")]
    if not active:
        console.print("\n[green]✓ No outliers found![/green]")
        return None

    total_records = sum(o.get("count", 1) for o in active)
    total_pages = (len(outliers) + page_size - 1) // page_size
    page = max(0, min(start_page, total_pages - 1)) if total_pages > 0 else 0

    while True:
        console.clear()
        console.print(
            f"[bold]Found {total_records} outlier(s) in {len(active)} group(s)[/bold]\n"
        )

        table = Table(show_header=True, header_style="bold", width=console.width)
        for col in ["#", "Entity", "Value", "Median", "Dev", "Count", "Latest"]:
            table.add_column(col, no_wrap=(col != "Entity"))

        start_idx = page * page_size
        for i, o in enumerate(
            outliers[start_idx : start_idx + page_size], start=start_idx + 1
        ):
            if o.get("_removed"):
                table.add_row(str(i), "[dim](removed)[/dim]", "", "", "", "", "")
                continue

            timestamps = o.get("timestamps", [o.get("timestamp")])
            latest_ts = max((ts for ts in timestamps if ts), default=None)

            # Format value range with arrow, using consistent decimal places
            if o.get("min_value") != o.get("max_value"):
                value_str = (
                    f"{format_number(o['min_value'])} → {format_number(o['max_value'])}"
                )
            else:
                value_str = format_number(o["value"])

            # Format count with percentage
            count = o.get("count", 1)
            total = o.get("total_samples", 0)
            pct = f" ({count * 100 / total:.1f}%)" if total else ""
            count_str = f"{count}{pct}"

            table.add_row(
                str(i),
                o["entity_id"],
                value_str,
                format_number(o["mean"]),
                f"{o['deviation']:.1f}σ",
                count_str,
                format_timestamp(latest_ts),
            )

        console.print(table)
        console.print(f"\nPage {page + 1}/{total_pages}")

        choice = (
            Prompt.ask("\[n]ext, \[p]rev, \[e]dit #, \[d]elete #, \[q]uit")
            .strip()
            .lower()
        )

        if choice == "n" and page < total_pages - 1:
            page += 1
        elif choice == "p" and page > 0:
            page -= 1
        elif choice == "q":
            break
        elif choice and choice[0] in "ed":
            try:
                num = int(choice[1:].strip())
                if 1 <= num <= len(outliers) and not outliers[num - 1].get("_removed"):
                    return (choice[0], num - 1, outliers[num - 1])
                console.print("[red]Invalid or already removed.[/red]")
            except ValueError:
                console.print("[red]Invalid format. Use: e5 or d5[/red]")
    return None


def _in_placeholders(items: list) -> str:
    return ",".join(["%s"] * len(items))


def edit_outlier(
    conn: mysql.connector.MySQLConnection, outlier: dict[str, Any]
) -> bool:
    """Edit outlier value(s) in the database."""
    ids = outlier.get("ids", [outlier["id"]])

    console.print("\n[bold]Editing outlier:[/bold]")
    console.print(f"  Entity: {outlier['entity_id']}")
    if outlier.get("min_value") != outlier.get("max_value"):
        console.print(
            f"  Current values: {format_number(outlier['min_value'])} – {format_number(outlier['max_value'])}"
        )
    else:
        console.print(f"  Current value: {format_number(outlier['value'])}")
    console.print(
        f"  Records: {len(ids)} | Mean: {format_number(outlier['mean'])} | Range: {format_number(outlier['lower_bound'])} – {format_number(outlier['upper_bound'])}"
    )

    choice = Prompt.ask("New value ('c' cancel, 'm' mean)").strip().lower()
    if choice == "c":
        return False

    try:
        new_value = outlier["mean"] if choice == "m" else float(choice)
    except ValueError:
        console.print("[red]Invalid number.[/red]")
        return False

    with conn.cursor() as cursor:
        try:
            cursor.execute(
                f"UPDATE states SET state = %s WHERE state_id IN ({_in_placeholders(ids)})",
                [str(new_value)] + ids,
            )
            conn.commit()
            console.print(f"[green]✓ Updated {cursor.rowcount} record(s)[/green]")
            return True
        except mysql.connector.Error as e:
            console.print(f"[red]✗ Database error: {e}[/red]")
            conn.rollback()
            return False


def delete_outlier(
    conn: mysql.connector.MySQLConnection, outlier: dict[str, Any]
) -> bool:
    """Delete outlier record(s) from the database."""
    ids = outlier.get("ids", [outlier["id"]])

    console.print("\n[bold]Deleting outlier:[/bold]")
    console.print(f"  Entity: {outlier['entity_id']}")
    if outlier.get("min_value") != outlier.get("max_value"):
        console.print(
            f"  Values: {format_number(outlier['min_value'])} – {format_number(outlier['max_value'])}"
        )
    else:
        console.print(f"  Value: {format_number(outlier['value'])}")
    console.print(f"  Records to delete: {len(ids)}")

    if not Confirm.ask("Are you sure?", default=False):
        return False

    with conn.cursor() as cursor:
        try:
            placeholders = _in_placeholders(ids)
            cursor.execute(
                f"UPDATE states SET old_state_id = NULL WHERE old_state_id IN ({placeholders})",
                ids,
            )
            cursor.execute(
                f"DELETE FROM states WHERE state_id IN ({placeholders})", ids
            )
            conn.commit()
            console.print(f"[green]✓ Deleted {cursor.rowcount} record(s)[/green]")
            return True
        except mysql.connector.Error as e:
            console.print(f"[red]✗ Database error: {e}[/red]")
            conn.rollback()
            return False


def interactive_config() -> dict:
    """Interactively get database configuration, using cached values as defaults."""
    console.print("\n[bold]=== Home Assistant Database Connection ===[/bold]\n")
    cached = load_cached_config() or {}
    defaults = {
        "host": "localhost",
        "port": 3306,
        "user": "homeassistant",
        "password": "",
        "database": "homeassistant",
    }
    defaults.update(cached)

    # Show stars for cached password
    pw_display = "*" * 10 if defaults["password"] else ""

    host = Prompt.ask("Host", default=defaults["host"])
    port = int(Prompt.ask("Port", default=str(defaults["port"])))
    user = Prompt.ask("User", default=defaults["user"])
    pw_input = Prompt.ask("Password", default=pw_display)
    # If user kept the stars or entered nothing, use cached password
    password = defaults["password"] if pw_input == pw_display else pw_input
    database = Prompt.ask("Database", default=defaults["database"])

    config = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }
    save_cached_config(config)
    return config


def main():
    """Main entry point."""
    signal.signal(signal.SIGINT, handle_interrupt)

    console.print("[bold]  Home Assistant MariaDB Outlier Detector[/bold]")

    config = interactive_config()

    def connect() -> mysql.connector.MySQLConnection:
        console.print(f"Connecting to {config['host']}:{config['port']}...", end=" ")
        c = get_db_connection(**config)
        console.print("[green]Connected![/green]")
        return c

    try:
        conn = connect()
    except mysql.connector.Error as e:
        console.print(f"\n[red]✗ Connection failed: {e}[/red]")
        sys.exit(1)

    console.print("\n[bold]Scanning states table for extreme outliers...[/bold]")
    try:
        all_outliers = group_outliers(find_outliers_in_states(conn))
        total = sum(o.get("count", 1) for o in all_outliers)
        console.print(f"  Found {total} outlier(s) in {len(all_outliers)} group(s)")
    except mysql.connector.Error as e:
        console.print(f"[red]Error: {e}[/red]")
        all_outliers = []

    current_page = 0
    while result := display_outliers(all_outliers, start_page=current_page):
        action, idx, outlier = result
        current_page = idx // PAGE_SIZE
        try:
            if (action == "e" and edit_outlier(conn, outlier)) or (
                action == "d" and delete_outlier(conn, outlier)
            ):
                all_outliers[idx]["_removed"] = True
        except mysql.connector.OperationalError:
            console.print("[yellow]Connection lost, reconnecting...[/yellow]")
            conn = connect()

    conn.close()
    console.print("\n[yellow]Goodbye![/yellow]")


if __name__ == "__main__":
    main()
