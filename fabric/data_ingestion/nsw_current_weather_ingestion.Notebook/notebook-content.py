# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3a264eb7-e398-469e-8398-865a7c1fe087",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "ff6a810c-a472-4e49-9583-835e0c5cb4fd",
# META       "known_lakehouses": [
# META         {
# META           "id": "3a264eb7-e398-469e-8398-865a7c1fe087"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "9f9d22bf-1868-a9c6-41bf-0ffcde324662",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS Bronze.weather_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% [markdown]
# # NSW Current Weather -> Bronze Ingestion
#
# Fans out one request per NSW location to the Google Weather API `currentConditions:lookup`
# endpoint, and appends a timestamped snapshot of all locations to a bronze Delta table.
#
# Docs: https://developers.google.com/maps/documentation/weather/current-conditions
#
# Notes on this API (different from a paginated REST source):
# - It's a point lookup (lat/lon in, current conditions out) - there is no "all of a region"
#   endpoint. Coverage of NSW means calling it once per location you care about.
# - There's nothing to page through - each call returns one snapshot for one point in time.
#   So the incremental pattern here is "append a fresh snapshot every run", not "resume from a
#   watermark".
#
# Before running: replace every <PLACEHOLDER>, and fill in / edit NSW_LOCATIONS below.

# %%
# ---- Parameters (replace placeholders) ----

# Auth - never hardcode the raw key. Pull it from a Key Vault-backed secret.
KEY_VAULT_URI = "<KEY_VAULT_URI>"          # e.g. "https://my-kv.vault.azure.net/"
API_KEY_SECRET_NAME = "<API_KEY_SECRET_NAME>"  # e.g. "google-weather-api-key"

# API
WEATHER_API_BASE_URL = "https://weather.googleapis.com/v1/currentConditions:lookup"
UNITS_SYSTEM = "METRIC"   # METRIC or IMPERIAL

# Reliability / rate limiting
REQUEST_TIMEOUT_SECS = 15
MAX_RETRIES = 5
BACKOFF_FACTOR_SECS = 2
DELAY_BETWEEN_CALLS_SECS = 0.2   # be polite to the API / stay under quota - tune to your QPS limit

# NSW locations to sample - edit/extend freely. This is a starting grid of major population
# centers, NOT an exhaustive list of "all NSW locations" (no such single source exists).
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
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, BooleanType
)

run_id = str(uuid.uuid4())
run_started_at = datetime.now(timezone.utc)
print(f"Run {run_id} started at {run_started_at.isoformat()}")

# %% [markdown]
# ## 1. Get the API key from Key Vault (never hardcode it)

# %%
# notebookutils is available by default in Fabric notebooks
# api_key = notebookutils.credentials.getSecret(KEY_VAULT_URI, API_KEY_SECRET_NAME)
api_key = "AIzaSyAM0tq7zAbWpBkUYQ1JTbqm2T0UNDj52X4"

# %% [markdown]
# ## 2. Fetch current conditions per location, with retry/backoff and per-location error isolation

# %%
def to_float(value):
    """Coerce numeric API values to float - Spark's explicit schema won't auto-upcast
    a plain int (e.g. 13) into a DoubleType column, so this avoids CANNOT_ACCEPT_OBJECT_IN_TYPE."""
    return float(value) if value is not None else None


def fetch_current_conditions(session, latitude, longitude):
    params = {
        "key": api_key,
        "location.latitude": latitude,
        "location.longitude": longitude,
        "unitsSystem": UNITS_SYSTEM,
    }

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(WEATHER_API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECS)
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"Retryable status {resp.status_code}: {resp.text[:200]}")
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            sleep_secs = BACKOFF_FACTOR_SECS ** attempt
            print(f"  attempt {attempt}/{MAX_RETRIES} failed: {exc}. Retrying in {sleep_secs}s")
            time.sleep(sleep_secs)
    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts") from last_exc


results = []

with requests.Session() as session:
    for loc in NSW_LOCATIONS:
        print(f"Fetching {loc['name']}...")
        row = {
            "location_name": loc["name"],
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
            "_ingested_at": run_started_at,
            "_run_id": run_id,
            "_status": "SUCCESS",
            "_error_message": None,
            "raw_json": None,
            "current_time": None,
            "condition_text": None,
            "temperature_c_or_f": None,
            "feels_like_temperature": None,
            "relative_humidity": None,
            "uv_index": None,
            "wind_speed": None,
            "wind_direction_cardinal": None,
            "precipitation_probability_pct": None,
            "cloud_cover_pct": None,
        }

        try:
            data = fetch_current_conditions(session, loc["latitude"], loc["longitude"])
            row["raw_json"] = json.dumps(data)
            row["current_time"] = data.get("currentTime")
            row["condition_text"] = data.get("weatherCondition", {}).get("description", {}).get("text")
            row["temperature_c_or_f"] = to_float(data.get("temperature", {}).get("degrees"))
            row["feels_like_temperature"] = to_float(data.get("feelsLikeTemperature", {}).get("degrees"))
            row["relative_humidity"] = data.get("relativeHumidity")
            row["uv_index"] = data.get("uvIndex")
            row["wind_speed"] = to_float(data.get("wind", {}).get("speed", {}).get("value"))
            row["wind_direction_cardinal"] = data.get("wind", {}).get("direction", {}).get("cardinal")
            row["precipitation_probability_pct"] = data.get("precipitation", {}).get("probability", {}).get("percent")
            row["cloud_cover_pct"] = data.get("cloudCover")
        except Exception as exc:
            # Isolate failures: one bad location doesn't stop the run or lose the others.
            row["_status"] = "FAILED"
            row["_error_message"] = str(exc)[:500]
            print(f"  {loc['name']} FAILED: {exc}")

        results.append(row)
        time.sleep(DELAY_BETWEEN_CALLS_SECS)

success_count = sum(1 for r in results if r["_status"] == "SUCCESS")
print(f"Done: {success_count}/{len(results)} locations succeeded")

# %% [markdown]
# ## 3. Write the snapshot to bronze (append-only time series)
#
# Every run adds one row per location for this point in time - that's the natural grain for a
# "current conditions" snapshot. `_run_id` / `_ingested_at` make each run identifiable and the
# write itself doesn't need a MERGE since we're never updating past snapshots, only adding new
# ones.

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
    StructField("current_time", StringType(), True),
    StructField("condition_text", StringType(), True),
    StructField("temperature_c_or_f", DoubleType(), True),
    StructField("feels_like_temperature", DoubleType(), True),
    StructField("relative_humidity", IntegerType(), True),
    StructField("uv_index", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction_cardinal", StringType(), True),
    StructField("precipitation_probability_pct", IntegerType(), True),
    StructField("cloud_cover_pct", IntegerType(), True),
])

snapshot_df = spark.createDataFrame(results, schema=schema)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_TABLE = "weather_data.bronze_nsw_current_weather"
if not spark.catalog.tableExists(BRONZE_TABLE):
    snapshot_df.write.format("delta").saveAsTable(BRONZE_TABLE)
    print(f"Created bronze table {BRONZE_TABLE} with {snapshot_df.count()} rows")
else:
    snapshot_df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    print(f"Appended {snapshot_df.count()} rows to {BRONZE_TABLE}")

# %% [markdown]
# ## 4. Run summary - surface failures instead of letting them go silent

# %%
failures = [r for r in results if r["_status"] == "FAILED"]
if failures:
    print(f"WARNING: {len(failures)} location(s) failed this run:")
    for f in failures:
        print(f"  - {f['location_name']}: {f['_error_message']}")
else:
    print("All locations succeeded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze.weather_data.bronze_nsw_current_weather LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
