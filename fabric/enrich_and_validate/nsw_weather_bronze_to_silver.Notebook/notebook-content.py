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
# META         },
# META         {
# META           "id": "7ce8f207-9318-4e04-bb82-ba0f80714e84"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "c5693ce9-906b-a5a6-40b6-f73c4f241634",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "7aeadcfe-3b75-4c6b-b757-9df87b2cfd65",
# META       "known_warehouses": [
# META         {
# META           "id": "7aeadcfe-3b75-4c6b-b757-9df87b2cfd65",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# %% [markdown]
# # NSW Weather: Bronze -> Silver
#
# Reads new bronze snapshots since the last silver run, cleans/validates/dedupes them, and
# MERGEs the result into a conformed silver table.
#
# Design choices:
# - Only `_status == "SUCCESS"` bronze rows are promoted. Failed-fetch rows stay in bronze
#   (full history/audit trail) but never reach silver.
# - Basic data-quality checks quarantine rows with impossible values (e.g. humidity outside
#   0-100) instead of silently letting bad data into silver or silently dropping it.
# - Dedup: if the same (location, current_time) reading was ingested more than once (e.g. a
#   rerun), silver keeps the most recently ingested copy.
# - Incremental: a watermark table tracks the max `_ingested_at` already processed, so each
#   run only reads new bronze rows rather than rescanning the whole table.
# - MERGE keyed on (location_name, current_time) makes reruns idempotent.
#
# Before running: replace every <PLACEHOLDER>.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Silver.silver_nsw_current_weather;
# MAGIC CREATE SCHEMA IF NOT EXISTS Silver.quarantine;
# MAGIC CREATE SCHEMA IF NOT EXISTS Silver.control

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# ---- Parameters (replace placeholders) ----

BRONZE_TABLE = "Bronze.weather_data.bronze_nsw_current_weather"
SILVER_TABLE = "Silver.silver_nsw_current_weather.silver_nsw_current_weather"
QUARANTINE_TABLE = "Silver.quarantine.quarantine_nsw_current_weather"
SILVER_WATERMARK_TABLE = "Silver.control.ctl_silver_watermark"
SILVER_STAGE_NAME = "nsw_current_weather"   # key identifying this bronze->silver job in the watermark table

# %%
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable

run_id = str(uuid.uuid4())
run_started_at = datetime.now(timezone.utc)
print(f"Silver run {run_id} started at {run_started_at.isoformat()}")

# %% [markdown]
# ## 1. Ensure watermark table exists, read last processed point

# %%
watermark_schema = StructType([
    StructField("stage_name", StringType(), False),
    StructField("last_ingested_at_processed", TimestampType(), False),
    StructField("last_run_id", StringType(), True),
])

if not spark.catalog.tableExists(SILVER_WATERMARK_TABLE):
    spark.createDataFrame([], watermark_schema).write.format("delta").saveAsTable(SILVER_WATERMARK_TABLE)
    print(f"Created watermark table {SILVER_WATERMARK_TABLE}")

watermark_row = (
    spark.table(SILVER_WATERMARK_TABLE)
    .filter(F.col("stage_name") == SILVER_STAGE_NAME)
    .orderBy(F.col("last_ingested_at_processed").desc())
    .limit(1)
    .collect()
)

last_watermark = watermark_row[0]["last_ingested_at_processed"] if watermark_row else datetime(1970, 1, 1, tzinfo=timezone.utc)
print(f"Processing bronze rows with _ingested_at > {last_watermark.isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# %% [markdown]
# ## 2. Read new bronze rows since the watermark

# %%
bronze_new = spark.table(BRONZE_TABLE).filter(F.col("_ingested_at") > F.lit(last_watermark))
new_row_count = bronze_new.count()
print(f"Found {new_row_count} new bronze row(s) to process")

if new_row_count == 0:
    print("Nothing new - skipping the rest of this run.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# %% [markdown]
# ## 3. Keep only successful fetches; parse/standardize types

# %%
if new_row_count > 0:
    parsed = (
        bronze_new
        .filter(F.col("_status") == "SUCCESS")
        .withColumn("location_name", F.trim(F.col("location_name")))
        .withColumn("current_time", F.to_timestamp("current_time"))
        .withColumn("condition_text", F.trim(F.col("condition_text")))
        .withColumn("wind_direction_cardinal", F.upper(F.trim(F.col("wind_direction_cardinal"))))
    )

    dropped_failed_count = bronze_new.filter(F.col("_status") != "SUCCESS").count()
    if dropped_failed_count:
        print(f"Skipping {dropped_failed_count} row(s) with _status != SUCCESS (they remain in bronze)")

# %% [markdown]
# ## 4. Data-quality checks - quarantine impossible values instead of dropping silently

# %%
if new_row_count > 0:
    is_valid = (
        F.col("current_time").isNotNull()
        & F.col("location_name").isNotNull()
        & (F.col("relative_humidity").between(0, 100) | F.col("relative_humidity").isNull())
        & (F.col("cloud_cover_pct").between(0, 100) | F.col("cloud_cover_pct").isNull())
        & (F.col("precipitation_probability_pct").between(0, 100) | F.col("precipitation_probability_pct").isNull())
        & (F.col("uv_index").between(0, 20) | F.col("uv_index").isNull())
        & (F.col("temperature_c_or_f").between(-90, 60) | F.col("temperature_c_or_f").isNull())
    )

    valid_rows = parsed.filter(is_valid)
    invalid_rows = parsed.filter(~is_valid).withColumn("_quarantine_reason", F.lit("failed_range_check"))

    invalid_count = invalid_rows.count()
    if invalid_count:
        print(f"Quarantining {invalid_count} row(s) that failed data-quality checks")
        invalid_rows.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(QUARANTINE_TABLE)

    print(f"{valid_rows.count()} row(s) passed validation")

# %% [markdown]
# ## 5. Dedup - keep the most recently ingested copy per (location, reading time)

# %%
if new_row_count > 0:
    dedup_window = Window.partitionBy("location_name", "current_time").orderBy(F.col("_ingested_at").desc())

    deduped = (
        valid_rows
        .withColumn("_row_rank", F.row_number().over(dedup_window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank", "raw_json", "_status", "_error_message")
    )

    print(f"{deduped.count()} row(s) after dedup, ready to merge into silver")

# %% [markdown]
# ## 6. MERGE into silver (idempotent on rerun)

# %%
if new_row_count > 0 and deduped.count() > 0:
    if not spark.catalog.tableExists(SILVER_TABLE):
        deduped.write.format("delta").saveAsTable(SILVER_TABLE)
        print(f"Created silver table {SILVER_TABLE} with {deduped.count()} rows")
    else:
        target = DeltaTable.forName(spark, SILVER_TABLE)
        (
            target.alias("t")
            .merge(
                deduped.alias("s"),
                "t.location_name = s.location_name AND t.current_time = s.current_time",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Merged {deduped.count()} row(s) into {SILVER_TABLE}")

# %% [markdown]
# ## 7. Advance the watermark (only after a successful write, using the max _ingested_at read)

# %%
if new_row_count > 0:
    max_ingested_at = bronze_new.agg(F.max("_ingested_at")).collect()[0][0]

    new_watermark = spark.createDataFrame(
        [(SILVER_STAGE_NAME, max_ingested_at, run_id)],
        schema=watermark_schema,
    )

    ctl = DeltaTable.forName(spark, SILVER_WATERMARK_TABLE)
    (
        ctl.alias("t")
        .merge(new_watermark.alias("s"), "t.stage_name = s.stage_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Watermark advanced to {max_ingested_at.isoformat()}")
else:
    print("Watermark unchanged - no new bronze rows this run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
