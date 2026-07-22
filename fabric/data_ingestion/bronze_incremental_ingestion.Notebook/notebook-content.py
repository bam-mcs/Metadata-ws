# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# %% [markdown]
# # Incremental REST -> Bronze Ingestion
#
# Reads a paginated REST API incrementally, with retry/backoff and idempotent Delta writes
# into the bronze layer.
#
# Before running: replace every <PLACEHOLDER> value in the Parameters cell.
#
# Pattern:
# 1. Read last watermark (page + timestamp) from a small Delta control table.
# 2. Page forward from there, with retry/backoff per request.
# 3. Stamp each row with ingestion metadata.
# 4. MERGE into bronze on a business key (safe to re-run - no duplicates).
# 5. Advance the watermark only after a successful write.
#
# Cell markers use "# %%" (Jupyter/VSCode style). If pasting back into Fabric, each
# "# %%" block is one notebook cell.

# %%
# ---- Parameters (replace placeholders) ----

# Source API
API_BASE_URL = "<API_BASE_URL>"            # e.g. "https://jsonplaceholder.typicode.com"
API_ENDPOINT_PATH = "<API_ENDPOINT_PATH>"  # e.g. "/posts"
PAGE_PARAM = "_page"                       # query param name for page number
PAGE_SIZE_PARAM = "_limit"                 # query param name for page size
PAGE_SIZE = 100
AUTH_HEADER_NAME = "<AUTH_HEADER_NAME>"    # e.g. "Authorization"; set to None if no auth
AUTH_HEADER_VALUE = "<AUTH_HEADER_VALUE>"  # e.g. "Bearer <token>"; pull from Key Vault in real use

# Safety / reliability
MAX_PAGES_PER_RUN = 500       # hard cap so a runaway API/loop can't run forever
REQUEST_TIMEOUT_SECS = 30
MAX_RETRIES = 5
BACKOFF_FACTOR_SECS = 2       # exponential backoff base

# Lakehouse targets - assumes this notebook is attached to the target Lakehouse's default context
BRONZE_TABLE = "<LAKEHOUSE_NAME>.bronze_<SOURCE_NAME>"
CONTROL_TABLE = "<LAKEHOUSE_NAME>.ctl_ingestion_watermark"
SOURCE_NAME = "<SOURCE_NAME>"               # unique key identifying this source in the control table
BUSINESS_KEY_COL = "<BUSINESS_KEY_COLUMN>"  # e.g. "id" - used for idempotent MERGE

# %%
import time
import uuid
import requests
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable

run_id = str(uuid.uuid4())
run_started_at = datetime.now(timezone.utc)
print(f"Run {run_id} started at {run_started_at.isoformat()}")

# %% [markdown]
# ## 1. Ensure control table exists, read last watermark

# %%
control_schema = StructType([
    StructField("source_name", StringType(), False),
    StructField("last_page_ingested", IntegerType(), False),
    StructField("last_run_id", StringType(), True),
    StructField("last_success_at", TimestampType(), True),
])

if not spark.catalog.tableExists(CONTROL_TABLE):
    empty_df = spark.createDataFrame([], control_schema)
    empty_df.write.format("delta").saveAsTable(CONTROL_TABLE)
    print(f"Created control table {CONTROL_TABLE}")

watermark_row = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("source_name") == SOURCE_NAME)
    .orderBy(F.col("last_success_at").desc())
    .limit(1)
    .collect()
)

start_page = (watermark_row[0]["last_page_ingested"] + 1) if watermark_row else 1
print(f"Resuming from page {start_page}")

# %% [markdown]
# ## 2. Fetch pages with retry/backoff
#
# Swap this loop's request logic if the real API uses cursor/token pagination instead of
# page numbers - carry the returned next_cursor forward instead of incrementing page.

# %%
def build_headers():
    headers = {"Accept": "application/json"}
    if AUTH_HEADER_NAME and AUTH_HEADER_VALUE and not AUTH_HEADER_NAME.startswith("<"):
        headers[AUTH_HEADER_NAME] = AUTH_HEADER_VALUE
    return headers

def fetch_page(session, page):
    url = f"{API_BASE_URL}{API_ENDPOINT_PATH}"
    params = {PAGE_PARAM: page, PAGE_SIZE_PARAM: PAGE_SIZE}

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, headers=build_headers(), timeout=REQUEST_TIMEOUT_SECS)
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"Retryable status {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException,) as exc:
            last_exc = exc
            sleep_secs = BACKOFF_FACTOR_SECS ** attempt
            print(f"Page {page} attempt {attempt}/{MAX_RETRIES} failed: {exc}. Retrying in {sleep_secs}s")
            time.sleep(sleep_secs)
    raise RuntimeError(f"Failed to fetch page {page} after {MAX_RETRIES} attempts") from last_exc

all_records = []
pages_fetched = 0
last_page_with_data = start_page - 1

with requests.Session() as session:
    page = start_page
    while pages_fetched < MAX_PAGES_PER_RUN:
        data = fetch_page(session, page)

        # Normalize: expect a list of records, or a dict wrapping a list - adjust key if needed
        records = data if isinstance(data, list) else data.get("<RESULTS_KEY>", [])

        if not records:
            print(f"Page {page} returned no records - stopping (end of data).")
            break

        all_records.extend(records)
        last_page_with_data = page
        pages_fetched += 1
        page += 1

print(f"Fetched {len(all_records)} records across {pages_fetched} page(s), "
      f"pages {start_page}-{last_page_with_data}")

# %% [markdown]
# ## 3. Stamp metadata and write to bronze (idempotent MERGE)

# %%
if not all_records:
    print("No new records this run - nothing to write.")
else:
    raw_df = spark.createDataFrame(all_records)

    bronze_df = (
        raw_df
        .withColumn("_ingested_at", F.lit(run_started_at))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_source_name", F.lit(SOURCE_NAME))
    )

    if not spark.catalog.tableExists(BRONZE_TABLE):
        bronze_df.write.format("delta").saveAsTable(BRONZE_TABLE)
        print(f"Created bronze table {BRONZE_TABLE} with {bronze_df.count()} rows")
    else:
        target = DeltaTable.forName(spark, BRONZE_TABLE)
        (
            target.alias("t")
            .merge(
                bronze_df.alias("s"),
                f"t.{BUSINESS_KEY_COL} = s.{BUSINESS_KEY_COL}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Merged {bronze_df.count()} rows into {BRONZE_TABLE}")

# %% [markdown]
# ## 4. Advance the watermark (only after a successful write)

# %%
if all_records:
    new_watermark = spark.createDataFrame(
        [(SOURCE_NAME, last_page_with_data, run_id, run_started_at)],
        schema=control_schema,
    )

    ctl = DeltaTable.forName(spark, CONTROL_TABLE)
    (
        ctl.alias("t")
        .merge(new_watermark.alias("s"), "t.source_name = s.source_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Watermark advanced to page {last_page_with_data} for source '{SOURCE_NAME}'")
else:
    print("Watermark unchanged - no data fetched this run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
