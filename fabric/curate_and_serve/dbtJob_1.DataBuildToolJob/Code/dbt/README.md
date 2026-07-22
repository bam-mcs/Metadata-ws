# NSW Current Weather — Gold Layer (dbt)

Builds a small dimensional model on top of `silver_nsw_current_weather` in your Silver
Lakehouse. Scoped down to current-weather only for now — the Air Quality and Forecast APIs
aren't reachable on a Google Maps demo key, so those sources are on hold until you have a
billing-enabled key. (The bronze/silver notebooks for those sources still exist in the
project if you want to pick this back up later — just point `sources.yml` at them again and
reintroduce the corresponding fact models.)

## Why a Warehouse, not the Lakehouse directly

Fabric Lakehouses expose a **read-only** SQL analytics endpoint — dbt can query it, but can't
`CREATE TABLE` or `MERGE` into it. dbt needs to materialize views/tables, so it needs a
read-write SQL surface. That's a **Fabric Warehouse**, not a Lakehouse.

```
Silver Lakehouse (Delta table, written by your PySpark notebook)
        │
        │  OneLake shortcut (zero-copy, read-only reference)
        ▼
Gold Warehouse (read-write SQL, this is where dbt runs)
        │
        │  dbt models (staging → marts)
        ▼
Gold tables (dim_location, dim_date, fct_current_weather_daily)
```

## One-time setup in Fabric (before running dbt)

1. **Create a Warehouse**, e.g. `Gold`.
2. Inside it: `New shortcut → OneLake → <workspace> → <Silver Lakehouse> → Tables →
   silver_nsw_current_weather`.
3. Note the schema the shortcut landed in (often `dbo`) — that's `<SILVER_SHORTCUT_SCHEMA>`
   in `models/staging/sources.yml`.
4. Grab the Warehouse's SQL connection string (Warehouse → Settings → SQL analytics endpoint).

## Local setup

```bash
pip install dbt-fabric
```

Requires the **ODBC Driver 18 for SQL Server** installed locally (a dbt-CLI dependency, not
a Fabric setting).

Copy `profiles.yml.example` to `~/.dbt/profiles.yml`, fill in your Warehouse's
server/database, pick interactive or service-principal auth.

```bash
dbt deps
dbt debug
dbt run
dbt test
```

## What gets built

| Layer | Model | Grain |
|---|---|---|
| staging | `stg_current_weather` | pass-through view |
| gold | `dim_location` | one row per location |
| gold | `dim_date` | one row per calendar day (static spine — widen the range in `dim_date.sql` as needed) |
| gold | `fct_current_weather_daily` | location × day, observed weather aggregated from snapshots |

## Re-adding air quality / forecast later

The removed pieces followed the exact same pattern as `stg_current_weather` /
`fct_current_weather_daily` — a staging pass-through plus a daily-aggregated fact, joined into
a combined mart via `dim_location` and `dim_date`. If you upgrade to a billing-enabled key,
ask for those models to be regenerated rather than trying to reverse-engineer them from memory.
