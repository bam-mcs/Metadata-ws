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

# %% [markdown]
# # NSW Current Air Quality -> Bronze Ingestion
#
# Fans out one request per NSW location to the Google Air Quality API `currentConditions:lookup`
# endpoint. This is a DIFFERENT API from the Weather API - different base URL, different auth
# placement, and it's a POST with a JSON body rather than a GET with query params.
#
# Docs: https://developers.google.com/maps/documentation/air-quality/current-conditions
#
# Like the weather current-conditions notebook, this is a live snapshot API - each run appends
# a fresh reading per location rather than resuming a backlog.
#
# Before running: replace every <PLACEHOLDER>. NSW_LOCATIONS is kept identical to the weather
# notebooks so gold can join weather and air quality by location_name.

# %%
# ---- Parameters (replace placeholders) ----

KEY_VAULT_URI = "<KEY_VAULT_URI>"
API_KEY_SECRET_NAME = "<AIR_QUALITY_API_KEY_SECRET_NAME>"  # may be the same GCP project key as Weather

AIR_QUALITY_URL = "https://airquality.googleapis.com/v1/currentConditions:lookup"

REQUEST_TIMEOUT_SECS = 15
MAX_RETRIES = 5
BACKOFF_FACTOR_SECS = 2
DELAY_BETWEEN_CALLS_SECS = 0.2   # Google's default quota is 6000 req/min, so this is generous headroom

BRONZE_TABLE = "Bronze.weather_data..bronze_nsw_air_quality_current"

# Common pollutant codes to flatten out individually if present in the response.
# See full list in the API's pollutant reference; extend this if you need more.
TRACKED_POLLUTANT_CODES = ["pm25", "pm10", "o3", "no2", "so2", "co"]

# Keep this identical to the weather notebooks' location list.
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
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

run_id = str(uuid.uuid4())
run_started_at = datetime.now(timezone.utc)
print(f"Run {run_id} started at {run_started_at.isoformat()}")

# api_key = notebookutils.credentials.getSecret(KEY_VAULT_URI, API_KEY_SECRET_NAME)
api_key = "AIzaSyAM0tq7zAbWpBkUYQ1JTbqm2T0UNDj52X4"


def to_float(value):
    return float(value) if value is not None else None


# %% [markdown]
# ## 1. Fetch air quality per location (POST with JSON body, not GET with query params)

# %%
def fetch_air_quality(session, latitude, longitude):
    url = f"{AIR_QUALITY_URL}?key={api_key}"
    body = {
        "location": {"latitude": latitude, "longitude": longitude},
        "universalAqi": True,
        "extraComputations": [
            "HEALTH_RECOMMENDATIONS",
            "POLLUTANT_CONCENTRATION",
            "DOMINANT_POLLUTANT_CONCENTRATION",
            "LOCAL_AQI",
        ],
    }

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(url, json=body, timeout=REQUEST_TIMEOUT_SECS)
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


def extract_pollutant_concentration(pollutants, code):
    for p in pollutants or []:
        if p.get("code") == code:
            return to_float(p.get("concentration", {}).get("value"))
    return None


results = []

with requests.Session() as session:
    for loc in NSW_LOCATIONS:
        print(f"Fetching air quality for {loc['name']}...")
        row = {
            "location_name": loc["name"],
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
            "_ingested_at": run_started_at,
            "_run_id": run_id,
            "_status": "SUCCESS",
            "_error_message": None,
            "raw_json": None,
            "reading_date_time": None,
            "universal_aqi": None,
            "universal_aqi_category": None,
            "dominant_pollutant": None,
        }
        for code in TRACKED_POLLUTANT_CODES:
            row[f"{code}_concentration"] = None

        try:
            data = fetch_air_quality(session, loc["latitude"], loc["longitude"])
            row["raw_json"] = json.dumps(data)
            row["reading_date_time"] = data.get("dateTime")

            indexes = data.get("indexes", [])
            uaqi = next((i for i in indexes if i.get("code") == "uaqi"), None)
            if uaqi:
                row["universal_aqi"] = uaqi.get("aqi")
                row["universal_aqi_category"] = uaqi.get("category")
                row["dominant_pollutant"] = uaqi.get("dominantPollutant")

            pollutants = data.get("pollutants", [])
            for code in TRACKED_POLLUTANT_CODES:
                row[f"{code}_concentration"] = extract_pollutant_concentration(pollutants, code)

        except Exception as exc:
            row["_status"] = "FAILED"
            row["_error_message"] = str(exc)[:500]
            print(f"  {loc['name']} FAILED: {exc}")

        results.append(row)
        time.sleep(DELAY_BETWEEN_CALLS_SECS)

success_count = sum(1 for r in results if r["_status"] == "SUCCESS")
print(f"Done: {success_count}/{len(results)} locations succeeded")

# %% [markdown]
# ## 2. Write the snapshot to bronze (append-only)

# %%
schema_fields = [
    StructField("location_name", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("_ingested_at", TimestampType(), False),
    StructField("_run_id", StringType(), False),
    StructField("_status", StringType(), False),
    StructField("_error_message", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("reading_date_time", StringType(), True),
    StructField("universal_aqi", IntegerType(), True),
    StructField("universal_aqi_category", StringType(), True),
    StructField("dominant_pollutant", StringType(), True),
] + [StructField(f"{code}_concentration", DoubleType(), True) for code in TRACKED_POLLUTANT_CODES]

schema = StructType(schema_fields)

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


if not spark.catalog.tableExists(BRONZE_TABLE):
    snapshot_df.write.format("delta").saveAsTable(BRONZE_TABLE)
    print(f"Created bronze table {BRONZE_TABLE} with {snapshot_df.count()} rows")
else:
    snapshot_df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    print(f"Appended {snapshot_df.count()} rows to {BRONZE_TABLE}")

# %% [markdown]
# ## 3. Run summary

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
