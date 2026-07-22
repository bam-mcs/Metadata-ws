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
# META       "default_lakehouse_workspace_id": "62cb089d-0592-41c9-a0c8-f658d407f812",
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

# MARKDOWN ********************

# ## Notebook to extract Azure Regulatory Policy Compliance via Azure Resource Graph.
# ### Prerequisite: Enterprise Application used must already been added to customer Lighthouse delegations.

# MARKDOWN ********************

# ## Define Parameters

# PARAMETERS CELL ********************

# Parameters
target_table_name = ''
source_query = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ensure Parameters are populated

# CELL ********************

missing_params = []

if not target_table_name:
    missing_params.append("target_table_name")
if not source_query:
    missing_params.append("source_query")

if missing_params:
    error_message = f"❌ Missing required parameters: {', '.join(missing_params)}"
    print(error_message)
    # Exit the notebook and signal failure to the pipeline
    mssparkutils.notebook.exit(
        f'{{"status": "Failed", "error": "{error_message}"}}'
    )
    raise SystemExit(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import modules

# CELL ********************

from typing import List, Dict, Any
import json
import time
from azure.identity import ClientSecretCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions
from pyspark.sql import SparkSession


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Obtain Enterprise App client Id and secret from Key Vault

# CELL ********************

# Get Key Vault secrets

def get_required_secret(vault_name: str, secret_name: str) -> str:
    value = mssparkutils.credentials.getSecret(vault_name, secret_name)
    if not value:
        raise ValueError(f"Secret '{secret_name}' was not found or is empty in Key Vault '{vault_name}'.")
    return value

key_vault_name = "https://mcs-automation-kv-01.vault.azure.net/"

connection_client_id = get_required_secret(key_vault_name, "devops-integration-clientid")
connection_client_secret = get_required_secret(key_vault_name, "devops-integration-appsecret")
tenantId = get_required_secret(key_vault_name, "webapp-sam-tenantid")

if not connection_client_id or not connection_client_secret or not tenantId:
    print("Error fetching Key Vault secrets!!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Batch extract using Azure Resource Graph skip token

# CELL ********************

# -------------------------
# Create Resource Graph Client
# -------------------------
credential = ClientSecretCredential(tenant_id=tenantId, client_id=connection_client_id, client_secret=connection_client_secret)
client = ResourceGraphClient(credential)

all_rows: List[Dict[str, Any]] = []
skip_token = None
page = 0

while True:
    page += 1
    print(f"Running query page {page} (skip_token={skip_token})...")

    options = QueryRequestOptions(skip_token=skip_token) if skip_token else None
    qreq = QueryRequest(query=source_query, options=options)

    resp = client.resources(qreq)

    # Attempt to convert to dict for robust access to fields across SDK versions
    try:
        resp_dict = resp.as_dict()
    except Exception:
        # fallback: try to JSON-serialize the response
        try:
            resp_dict = json.loads(resp._response.text)
        except Exception:
            raise RuntimeError("Could not parse Resource Graph response")

    # The response payload typically contains 'data' (list of rows) and a 'skipToken' for pagination.
    # Handle case-insensitive keys for robustness
    data = resp_dict.get('data') or resp_dict.get('value') or resp_dict.get('results')
    if data is None:
        # Some SDK versions wrap results under 'result' or other keys. Try to find lists in the dict.
        for v in resp_dict.values():
            if isinstance(v, list):
                data = v
                break

    if not data:
        print("No rows returned by Resource Graph.")
        break

    # Append rows
    # If rows are strings (JSON), attempt to parse each row
    parsed_rows = []
    for r in data:
        if isinstance(r, str):
            try:
                parsed_rows.append(json.loads(r))
            except Exception:
                parsed_rows.append({"raw": r})
        else:
            parsed_rows.append(r)

    all_rows.extend(parsed_rows)

    # Reset skip_token
    skip_token = None
    # Get skip token for next page. Try several common key names.
    skip_token = resp_dict.get('skipToken') or resp_dict.get('skip_token') or resp_dict.get('$skipToken') or None

    if not skip_token:
        # No more pages
        break
    else:
        # small sleep to be polite to API (adjust if needed)
        time.sleep(0.5)


print(f"Total rows fetched: {len(all_rows)}")

if len(all_rows) == 0:
    print("No data returned from query. Exiting.")
else:
    # Convert list-of-dicts to spark DataFrame
    df = spark.createDataFrame(all_rows)
    display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Perform incremental updates on the Lakehouse table instead of overwrite/append

# CELL ********************

try:
    existing_df = spark.sql(f"SELECT * FROM Bronze.dbo.{target_table_name}")
except Exception:
    existing_df = None

if existing_df:
    # Combine both dataframes
    combined_df = existing_df.unionByName(df)
    # Dedupe
    combined_df = combined_df.dropDuplicates()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write or merge into Bronze Lakehouse

# CELL ********************

# Merge the changes into the Lakehouse Delta table
if existing_df:
    print(f"Writing combined and deduped dataframe to Lakehouse table {target_table_name}")
    combined_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(target_table_name)
# Write to Bronze Lakehouse
else:
    print(f"Writing to Lakehouse table {target_table_name}")
    df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(target_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Output to next pipeline activity

# CELL ********************

from notebookutils import mssparkutils

# calculate rows read
if existing_df:
    rows_read = existing_df.count()
else:
    rows_read = df.count()

# calculate rows copied or processed
if existing_df:
    rows_copied = combined_df.count()
else:
    rows_copied = df.count()

# build data consistency check result
verification_result = {
    "status": "Passed" if rows_copied > 0 else "Failed",
    "rowCount": rows_copied
}

# wrap into expected Fabric pipeline output structure
output_json = {
    "rowsRead": rows_read,
    "rowsCopied": rows_copied,
    "dataConsistencyVerification": {
        "VerificationResult": verification_result
    }
}

# return structured JSON to pipeline
mssparkutils.notebook.exit(json.dumps(output_json))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
