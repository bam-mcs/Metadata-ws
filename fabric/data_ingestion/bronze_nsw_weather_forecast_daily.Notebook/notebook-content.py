# Fabric notebook source


# CELL ********************

# %% [markdown]
# # NSW Daily Weather Forecast -> Bronze Ingestion
#
# Fans out one request per NSW location to the Google Weather API `forecast/days:lookup`
# endpoint (10-day forecast), paginating within each location's response via `pageToken`.
#
# Docs: https://developers.google.com/maps/documentation/weather/daily-forecast
#
# Two levels of iteration here, don't confuse them:
# - OUTER loop: one location after another (fan-out, same as current-conditions notebook)
# - INNER loop: page through that location's forecast days if the API splits them across
#   multiple pages (governed by `pageToken` / `nextPageToken`)
#
# Like current conditions, this is a snapshot-in-time API (today's forecast for the next N
# days) - each run appends a fresh forecast snapshot rather than resuming a backlog.
#
# Before running: replace every <PLACEHOLDER>, and confirm NSW_LOCATIONS matches what you're
# using in the current-conditions notebook (keep location names consistent across sources -
# gold will join on location_name).

# %%
# ---- Parameters (replace placeholders) ----

KEY_VAULT_URI = "<KEY_VAULT_URI>"
API_KEY_SECRET_NAME = "<API_KEY_SECRET_NAME>"   # same Weather API key as current conditions

WEATHER_FORECAST_URL = "https://weather.googleapis.com/v1/forecast/days:lookup"
FORECAST_DAYS = 10          # Google's max is 10
FORECAST_PAGE_SIZE = 10     # ask for all days in one page where possible

REQUEST_TIMEOUT_SECS = 15
MAX_RETRIES = 5
BACKOFF_FACTOR_SECS = 2
DELAY_BETWEEN_CALLS_SECS = 0.2
MAX_PAGES_PER_LOCATION = 5   # safety cap on the inner pagination loop

BRONZE_TABLE = "<LAKEHOUSE_NAME>.bronze_nsw_weather_forecast_daily"

# Keep this identical to the current-conditions notebook's list so gold can join cleanly.
NSW_LOCATIONS = [
    {"name": "Sydney",         "latitude": -33.8688, "longitude": 151.2093},
    {"name": "Newcastle",      "latitude": -32.9283, "longitude": 151.7817},
    {"name": "Wollongong",     "latitude": -34.4278, "longitude": 150.8931},
    {"name": "Wagga Wagga",    "latitude": -35.1082, "longitude": 147.3598},
    {"name": "Albury",         "latitude": -36.0737, "longitude": 146.9135},
    {"name": "Dubbo",          "latitude": -32.2569, "longitude": 148.6011},
    {"name": "Orange",         "latitude": -33.2839, "longitude": 149.1000},
    {"name": "Bathurst",       "latitude": -33.4193, "longitude": 149.5775},
    {"name": "Tamworth",       "latitude": -31.0927, "longitude": 150.9187},
    {"name": "Coffs Harbour",  "latitude": -30.2963, "longitude": 153.1157},
    {"name": "Port Macquarie", "latitude": -31.4333, "longitude": 152.9094},
    {"name": "Broken Hill",    "latitude": -31.9539, "longitude": 141.4539},
    {"name": "Griffith",       "latitude": -34.2900, "longitude": 146.0530},
    {"name": "Lismore",        "latitude": -28.8135, "longitude": 153.2775},
    {"name": "Nowra",          "latitude": -34.8797, "longitude": 150.6017},
]

# %%
import time
import uuid
import json
import requests
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, DateType
)

run_id = str(uuid.uuid4())
run_started_at = datetime.now(timezone.utc)
print(f"Run {run_id} started at {run_started_at.isoformat()}")

api_key = notebookutils.credentials.getSecret(KEY_VAULT_URI, API_KEY_SECRET_NAME)


def to_float(value):
    return float(value) if value is not None else None


# %% [markdown]
# ## 1. Fetch each location's forecast, paging within the location as needed

# %%
def fetch_forecast_page(session, latitude, longitude, page_token=None):
    params = {
        "key": api_key,
        "location.latitude": latitude,
        "location.longitude": longitude,
        "days": FORECAST_DAYS,
        "pageSize": FORECAST_PAGE_SIZE,
    }
    if page_token:
        params["pageToken"] = page_token

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(WEATHER_FORECAST_URL, params=params, timeout=REQUEST_TIMEOUT_SECS)
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"Retryable status {resp.status_code}: {resp.text[:200]}")
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            sleep_secs = BACKOFF_FACTOR_SECS ** attempt
            print(f"    attempt {attempt}/{MAX_RETRIES} failed: {exc}. Retrying in {sleep_secs}s")
            time.sleep(sleep_secs)
    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts") from last_exc


forecast_rows = []

with requests.Session() as session:
    for loc in NSW_LOCATIONS:
        print(f"Fetching forecast for {loc['name']}...")
        try:
            page_token = None
            pages_fetched = 0
            forecast_days_for_location = []

            while pages_fetched < MAX_PAGES_PER_LOCATION:
                data = fetch_forecast_page(session, loc["latitude"], loc["longitude"], page_token)
                days = data.get("forecastDays", [])
                forecast_days_for_location.extend(days)
                pages_fetched += 1

                page_token = data.get("nextPageToken")
                if not page_token:
                    break

            for day in forecast_days_for_location:
                display_date = day.get("displayDate", {})
                daytime = day.get("daytimeForecast", {}) or {}
                nighttime = day.get("nighttimeForecast", {}) or {}

                forecast_rows.append({
                    "location_name": loc["name"],
                    "latitude": loc["latitude"],
                    "longitude": loc["longitude"],
                    "_ingested_at": run_started_at,
                    "_run_id": run_id,
                    "_status": "SUCCESS",
                    "_error_message": None,
                    "raw_json": json.dumps(day),
                    "forecast_date": (
                        f"{display_date.get('year')}-{display_date.get('month'):02d}-{display_date.get('day'):02d}"
                        if display_date.get("year") else None
                    ),
                    "interval_start": day.get("interval", {}).get("startTime"),
                    "interval_end": day.get("interval", {}).get("endTime"),
                    "max_temperature": to_float(day.get("maxTemperature", {}).get("degrees")),
                    "min_temperature": to_float(day.get("minTemperature", {}).get("degrees")),
                    "daytime_condition_text": daytime.get("weatherCondition", {}).get("description", {}).get("text"),
                    "daytime_precip_probability_pct": daytime.get("precipitation", {}).get("probability", {}).get("percent"),
                    "daytime_relative_humidity": daytime.get("relativeHumidity"),
                    "daytime_uv_index": daytime.get("uvIndex"),
                    "daytime_wind_speed": to_float(daytime.get("wind", {}).get("speed", {}).get("value")),
                    "nighttime_condition_text": nighttime.get("weatherCondition", {}).get("description", {}).get("text"),
                    "nighttime_precip_probability_pct": nighttime.get("precipitation", {}).get("probability", {}).get("percent"),
                    "nighttime_relative_humidity": nighttime.get("relativeHumidity"),
                })

        except Exception as exc:
            # One location failing doesn't stop the others.
            print(f"  {loc['name']} FAILED: {exc}")
            forecast_rows.append({
                "location_name": loc["name"],
                "latitude": loc["latitude"],
                "longitude": loc["longitude"],
                "_ingested_at": run_started_at,
                "_run_id": run_id,
                "_status": "FAILED",
                "_error_message": str(exc)[:500],
                "raw_json": None,
                "forecast_date": None,
                "interval_start": None,
                "interval_end": None,
                "max_temperature": None,
                "min_temperature": None,
                "daytime_condition_text": None,
                "daytime_precip_probability_pct": None,
                "daytime_relative_humidity": None,
                "daytime_uv_index": None,
                "daytime_wind_speed": None,
                "nighttime_condition_text": None,
                "nighttime_precip_probability_pct": None,
                "nighttime_relative_humidity": None,
            })

        time.sleep(DELAY_BETWEEN_CALLS_SECS)

success_count = sum(1 for r in forecast_rows if r["_status"] == "SUCCESS")
print(f"Done: {success_count}/{len(forecast_rows)} forecast-day row(s) fetched successfully")

# %% [markdown]
# ## 2. Write the snapshot to bronze (append-only - each run is "today's forecast as of now")

# %%
schema = StructType([
    StructField("location_name", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("_ingested_at", TimestampType(), False),
    StructField("_run_id", StringType(), False),
    StructField("_status", StringType(), False),
    StructField("_error_message", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("forecast_date", StringType(), True),
    StructField("interval_start", StringType(), True),
    StructField("interval_end", StringType(), True),
    StructField("max_temperature", DoubleType(), True),
    StructField("min_temperature", DoubleType(), True),
    StructField("daytime_condition_text", StringType(), True),
    StructField("daytime_precip_probability_pct", IntegerType(), True),
    StructField("daytime_relative_humidity", IntegerType(), True),
    StructField("daytime_uv_index", IntegerType(), True),
    StructField("daytime_wind_speed", DoubleType(), True),
    StructField("nighttime_condition_text", StringType(), True),
    StructField("nighttime_precip_probability_pct", IntegerType(), True),
    StructField("nighttime_relative_humidity", IntegerType(), True),
])

snapshot_df = spark.createDataFrame(forecast_rows, schema=schema)

if not spark.catalog.tableExists(BRONZE_TABLE):
    snapshot_df.write.format("delta").saveAsTable(BRONZE_TABLE)
    print(f"Created bronze table {BRONZE_TABLE} with {snapshot_df.count()} rows")
else:
    snapshot_df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    print(f"Appended {snapshot_df.count()} rows to {BRONZE_TABLE}")

# %% [markdown]
# ## 3. Run summary

# %%
failures = [r for r in forecast_rows if r["_status"] == "FAILED"]
if failures:
    print(f"WARNING: {len(failures)} location(s) failed this run:")
    for f in failures:
        print(f"  - {f['location_name']}: {f['_error_message']}")
else:
    print("All locations succeeded.")
