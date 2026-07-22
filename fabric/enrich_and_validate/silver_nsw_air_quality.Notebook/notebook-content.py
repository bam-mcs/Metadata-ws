# Fabric notebook source


# CELL ********************

# %% [markdown]
# # NSW Air Quality: Bronze -> Silver
#
# Same bronze->silver pattern as the other two sources. Dedup key is
# (location_name, reading_date_time).
#
# Before running: replace every <PLACEHOLDER>.

# %%
BRONZE_TABLE = "<LAKEHOUSE_NAME>.bronze_nsw_air_quality_current"
SILVER_TABLE = "<LAKEHOUSE_NAME>.silver_nsw_air_quality"
QUARANTINE_TABLE = "<LAKEHOUSE_NAME>.quarantine_nsw_air_quality"
SILVER_WATERMARK_TABLE = "<LAKEHOUSE_NAME>.ctl_silver_watermark"
SILVER_STAGE_NAME = "nsw_air_quality_current"

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
# ## 1. Watermark

# %%
watermark_schema = StructType([
    StructField("stage_name", StringType(), False),
    StructField("last_ingested_at_processed", TimestampType(), False),
    StructField("last_run_id", StringType(), True),
])

if not spark.catalog.tableExists(SILVER_WATERMARK_TABLE):
    spark.createDataFrame([], watermark_schema).write.format("delta").saveAsTable(SILVER_WATERMARK_TABLE)

watermark_row = (
    spark.table(SILVER_WATERMARK_TABLE)
    .filter(F.col("stage_name") == SILVER_STAGE_NAME)
    .orderBy(F.col("last_ingested_at_processed").desc())
    .limit(1)
    .collect()
)
last_watermark = watermark_row[0]["last_ingested_at_processed"] if watermark_row else datetime(1970, 1, 1, tzinfo=timezone.utc)
print(f"Processing bronze rows with _ingested_at > {last_watermark.isoformat()}")

# %% [markdown]
# ## 2. Read new bronze rows

# %%
bronze_new = spark.table(BRONZE_TABLE).filter(F.col("_ingested_at") > F.lit(last_watermark))
new_row_count = bronze_new.count()
print(f"Found {new_row_count} new bronze row(s)")

# %% [markdown]
# ## 3. Keep successes only, parse types

# %%
if new_row_count > 0:
    parsed = (
        bronze_new
        .filter(F.col("_status") == "SUCCESS")
        .withColumn("location_name", F.trim(F.col("location_name")))
        .withColumn("reading_date_time", F.to_timestamp("reading_date_time"))
        .withColumn("universal_aqi_category", F.trim(F.col("universal_aqi_category")))
        .withColumn("dominant_pollutant", F.trim(F.col("dominant_pollutant")))
    )

    dropped_failed_count = bronze_new.filter(F.col("_status") != "SUCCESS").count()
    if dropped_failed_count:
        print(f"Skipping {dropped_failed_count} row(s) with _status != SUCCESS")

# %% [markdown]
# ## 4. Data-quality checks -> quarantine
#
# The Universal AQI scale is 0-500 (per Google's AQI documentation); pollutant concentrations
# are non-negative. Adjust the AQI ceiling if you switch to a different regional AQI scale.

# %%
if new_row_count > 0:
    is_valid = (
        F.col("reading_date_time").isNotNull()
        & F.col("location_name").isNotNull()
        & (F.col("universal_aqi").between(0, 500) | F.col("universal_aqi").isNull())
        & ((F.col("pm25_concentration") >= 0) | F.col("pm25_concentration").isNull())
        & ((F.col("pm10_concentration") >= 0) | F.col("pm10_concentration").isNull())
        & ((F.col("o3_concentration") >= 0) | F.col("o3_concentration").isNull())
        & ((F.col("no2_concentration") >= 0) | F.col("no2_concentration").isNull())
        & ((F.col("so2_concentration") >= 0) | F.col("so2_concentration").isNull())
        & ((F.col("co_concentration") >= 0) | F.col("co_concentration").isNull())
    )

    valid_rows = parsed.filter(is_valid)
    invalid_rows = parsed.filter(~is_valid).withColumn("_quarantine_reason", F.lit("failed_range_check"))

    invalid_count = invalid_rows.count()
    if invalid_count:
        print(f"Quarantining {invalid_count} row(s)")
        invalid_rows.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(QUARANTINE_TABLE)

    print(f"{valid_rows.count()} row(s) passed validation")

# %% [markdown]
# ## 5. Dedup - most recently ingested reading per (location, reading_date_time)

# %%
if new_row_count > 0:
    dedup_window = Window.partitionBy("location_name", "reading_date_time").orderBy(F.col("_ingested_at").desc())

    deduped = (
        valid_rows
        .withColumn("_row_rank", F.row_number().over(dedup_window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank", "raw_json", "_status", "_error_message")
    )

    print(f"{deduped.count()} row(s) after dedup")

# %% [markdown]
# ## 6. MERGE into silver

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
                "t.location_name = s.location_name AND t.reading_date_time = s.reading_date_time",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Merged {deduped.count()} row(s) into {SILVER_TABLE}")

# %% [markdown]
# ## 7. Advance watermark

# %%
if new_row_count > 0:
    max_ingested_at = bronze_new.agg(F.max("_ingested_at")).collect()[0][0]
    new_watermark = spark.createDataFrame([(SILVER_STAGE_NAME, max_ingested_at, run_id)], schema=watermark_schema)

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
