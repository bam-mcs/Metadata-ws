# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Import modules

# CELL ********************

from pyspark.sql import SparkSession
from datetime import datetime
import json
import requests
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper functions

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# ==========================================================
# CONFIGURATION
# ==========================================================
workspace_id = fabric.get_workspace_id()
base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"

# ==========================================================
# AUTHENTICATION
# ==========================================================
def get_auth_header():
    """Return Fabric API bearer token for current user/session"""
    token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
    return {"Authorization": f"Bearer {token}"}

# ==========================================================
# HELPER FUNCTIONS
# ==========================================================
def get_items_in_workspace():
    """List all items (e.g., Lakehouses) in the workspace"""
    url = f"{base_url}/items"
    resp = requests.get(url, headers=get_auth_header())
    if resp.status_code != 200:
        raise Exception(f"Error retrieving items: {resp.status_code} {resp.text}")
    return resp.json().get("value", [])

def get_shortcuts_for_lakehouse(lakehouse_id):
    """List all shortcuts for a given Lakehouse"""
    url = f"{base_url}/items/{lakehouse_id}/shortcuts"
    resp = requests.get(url, headers=get_auth_header())
    if resp.status_code != 200:
        raise Exception(f"Error retrieving shortcuts: {resp.status_code} {resp.text}")
    return resp.json().get("value", [])

def get_fabric_json(url):
    r = requests.get(url, headers=get_auth_header())
    if r.status_code == 200:
        return r.json()
    else:
        print(f"⚠️ API call failed: {r.status_code} - {r.text}")
        return None

def find_key(obj, key):
    """Return first occurrence of key in a nested dict/list."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() == key.lower():
                return v
            result = find_key(v, key)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_key(item, key)
            if result is not None:
                return result
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Audit current workspace

# CELL ********************

workspace_items = get_items_in_workspace()

all_rows = []
for item in workspace_items:
    if item.get("type") == "Lakehouse":
        target_lakehouse_name = item.get("displayName")
        lakehouse_id = item.get("id")

        shortcuts = get_shortcuts_for_lakehouse(lakehouse_id)
        for s in shortcuts:
            subpath = find_key(s, "subpath")
            target = s.get("target", {})
            oneLake = target.get("oneLake", {})

            source_type = target.get("type")
            source_item_id = oneLake.get("itemId")
            source_workspace_id = oneLake.get("workspaceId")

            # Defaults
            source_workspace_name = None
            source_item_name = None
            source_item_type = None

            # ----------------------------------------------------------------
            # If source_type is OneLake → get Lakehouse details
            # ----------------------------------------------------------------
            if source_type and source_type.lower() == "onelake" and source_workspace_id and source_item_id:
                # Get source workspace details
                workspace_url = f"https://api.fabric.microsoft.com/v1/workspaces/{source_workspace_id}"
                workspace_data = get_fabric_json(workspace_url)
                if workspace_data:
                    source_workspace_name = workspace_data.get("displayName")

                # Get source Lakehouse details
                lakehouse_url = f"https://api.fabric.microsoft.com/v1/workspaces/{source_workspace_id}/lakehouses/{source_item_id}"
                lakehouse_data = get_fabric_json(lakehouse_url)
                if lakehouse_data:
                    source_item_name = lakehouse_data.get("displayName")
                    source_item_type = "lakehouse"

            all_rows.append({
                "shortcut_name": s.get("name"),
                "shortcut_path": s.get("path"),
                "source_type": source_type,
                "target_lakehouse_name": target_lakehouse_name,
                "medallion_layer": (
                    "bronze" if "bronze" in target_lakehouse_name.lower()
                    else "silver" if "silver" in target_lakehouse_name.lower()
                    else "gold" if "gold" in target_lakehouse_name.lower()
                    else None
                ),
                "onelake_path": oneLake.get("path"),
                "source_item_id": source_item_id,
                "source_item_name": source_item_name,
                "source_item_type": source_item_type,
                "source_workspace_id": source_workspace_id,
                "source_workspace_name": source_workspace_name,
                "subpath": subpath,
                "shortcut_audit_refreshtime": datetime.utcnow().isoformat()
            })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Convert to dataframe and output to JSON

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType

# Define explicit schema (all fields as StringType for safety)
schema = StructType([
    StructField("shortcut_name", StringType(), True),
    StructField("shortcut_path", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("target_lakehouse_name", StringType(), True),
    StructField("medallion_layer", StringType(), True),
    StructField("onelake_path", StringType(), True),
    StructField("source_item_id", StringType(), True),
    StructField("source_item_name", StringType(), True),
    StructField("source_item_type", StringType(), True),
    StructField("source_workspace_id", StringType(), True),
    StructField("source_workspace_name", StringType(), True),
    StructField("subpath", StringType(), True),
    StructField("shortcut_audit_refreshtime", StringType(), True)
])

if all_rows:
    df = spark.createDataFrame(all_rows, schema=schema)
    display(df)
else:
    print("No shortcuts found in this workspace.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Output to pipeline

# CELL ********************

# Convert nulls to empty spaces
df = df.fillna("")

# Collect DataFrame rows as a list of dicts
result = [json.loads(r) for r in df.toJSON().collect()]

# Exit as a clean JSON string
mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
