# Home Assistant Outlier Detector

Home Assistant records every entity state change to it's database. Over time, however, sensor glitches, integration bugs, or network issues can introduce  extreme values that throw off your statistics and graphs. A temperature sensor briefly reporting 9,999°C or a power meter showing negative consumption might only appear once, but that single bad reading can skew long-term averages and make a mess of your dashboard visualisations.

This tool scans your Home Assistant MariaDB database for statistical outliers—values that fall more than 5 standard deviations from the mean—and lets you review, edit, or delete them interactively.

## Why not use Home Assistant's built-in statistics adjustment?

Home Assistant does include a feature for adjusting long-term statistics (found under Developer Tools → Statistics). It's useful for correcting cumulative sensor errors—say, when your energy meter resets unexpectedly and suddenly claims you've consumed -50,000 kWh.

However, that feature operates solely on the **long-term statistics** tables (`statistics` and `statistics_short_term`), which store hourly and five-minute aggregated data respectively. It doesn't touch the **states** table.

The states table contains your raw, unaggregated sensor history. If a sensor briefly went bad and logged a few incorrect values, those readings remain in the states table forever, even after you've tidied up the statistics. This matters because:

- **History graphs** (the standard entity history card) pull directly from the states table, so you'll still see those unsightly spikes
- **Template sensors** or automations that query historical state data will encounter the bogus values
- **Future statistics recalculations** may reintroduce the outliers if Home Assistant ever regenerates the aggregated data

This tool addresses that gap by letting you hunt down and correct (or remove) the offending records in the states table itself.

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Network access to your Home Assistant MariaDB/MySQL database

## Usage

```bash
uv run ha_outliers.py
```

On first run, you'll be prompted for database connection details (host, port, user, password, database). These are cached in `~/.config/ha-outliers/config.json` for subsequent runs.

The tool scans all numeric sensor entities and presents outliers in a table, grouped by entity and severity. From there you can:

- **Navigate** with `n` (next page) and `p` (previous page)
- **Edit** a value with `e<number>` (e.g., `e5` to edit row 5)—you can enter a new value or press `m` to replace with the median
- **Delete** a record with `d<number>` (e.g., `d5`)
- **Quit** with `q`

## How it works

The scanner queries each numeric entity's historical states, calculating mean and standard deviation directly in the database. Values beyond 5σ from the mean are flagged as potential outliers. To reduce noise, values appearing in more than 1% of an entity's samples are excluded (these are likely legitimate recurring states rather than glitches).

Outliers are grouped by entity, direction (above/below median), and severity band to make bulk operations rather more straightforward. A single edit or delete action can affect multiple database records if they fall within the same group.

## Database permissions

The tool requires SELECT, UPDATE, and DELETE permissions on the `states` and `states_meta` tables. If you only wish to review outliers without modifying them, SELECT permission alone will suffice.

## Caution

This tool modifies your Home Assistant database directly. Do make a backup before deleting or editing records—deleted state history cannot be recovered, and you'll have no one to blame but yourself if things go pear-shaped.

## Is there a sqlite version?

No, unfortunately there is no sqlite version available. Patches welcome!