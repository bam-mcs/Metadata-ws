# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7ce8f207-9318-4e04-bb82-ba0f80714e84",
# META       "default_lakehouse_name": "Silver",
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
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# ## Set Parameters

# PARAMETERS CELL ********************

azure_key_vault = ''
azure_key_vault_secret_client_id_name = ''
azure_key_vault_secret_client_secret_name = ''

source_type = ''
source_item_name = ''
source_connection_url = ''

transformation_flag = ''
transformation_rule = ''
transformation_rule_criteria = ''

target_item_name = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import modules

# CELL ********************

import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from collections import Counter
from notebookutils import mssparkutils
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType
from dateutil import parser

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Functions

# CELL ********************

# Aggregate - join tables
def apply_dynamic_join(source_df, rule_str):
    """
    Parses and applies a dynamic join based on a rule string.
    
    Example rule:
      'join:inner|Bronze.dbo.Managed_Azure_Regulatory_Compliance_Policies_Raw|
       t1.policyDefinitionId=t2.policyDefinitionId;t2.policySetDefinitionId=t1.policySetDefinitionId'
    
    Returns:
      (joined_df, details_dict)
    """
    # --- Split into sections
    parts = rule_str.split("|")
    if len(parts) < 3:
        raise ValueError(f"Invalid rule format: {rule_str}")

    join_type = parts[0].split(":")[1].strip()           # e.g. 'inner'
    join_item_name = parts[1].strip()                    # e.g. 'Bronze.dbo.TableName'
    join_criteria_str = parts[2].strip()                 # e.g. 't1.col=t2.col;t2.colB=t1.colB'

    # --- Parse criteria (multiple separated by ;)
    criteria_list = [c.strip() for c in join_criteria_str.split(";") if c.strip()]

    # --- Identify and rename only conflicting columns in t2 before join
    # dataframes allows duplicate columns after join but Lakehouse does not
    join_table_df = spark.sql(f"SELECT * FROM {join_item_name}")
    common_cols = set(source_df.columns).intersection(set(join_table_df.columns))
    renamed_cols_map = {}
    if common_cols:
        print(f"Renaming {len(common_cols)} duplicate columns from t2:", common_cols)
        for col_name in common_cols:
            new_name = f"{col_name}2"
            join_table_df = join_table_df.withColumnRenamed(col_name, new_name)
            renamed_cols_map[col_name] = new_name

    # --- Build join condition respecting prefixes (t1/t2)
    join_condition = None
    for condition_str in criteria_list:
        left, right = condition_str.split("=")
        left_table, left_col = left.split(".")
        right_table, right_col = right.split(".")

        left_table, left_col = left_table.strip(), left_col.strip()
        right_table, right_col = right_table.strip(), right_col.strip()

        # Replace right_col with renamed version if renamed
        if right_table == "t2" and right_col in renamed_cols_map:
            right_col = renamed_cols_map[right_col]

        left_expr = F.col(f"{left_table}.{left_col}")
        right_expr = F.col(f"{right_table}.{right_col}")

        condition = F.trim(F.lower(left_expr)) == F.trim(F.lower(right_expr))
        join_condition = condition if join_condition is None else join_condition & condition

    # --- Apply join dynamically
    aggr_df = (
        join_table_df.alias("t2")
        .join(source_df.alias("t1"), on=join_condition, how=join_type)
    )

    # --- Return both joined DataFrame and parsed metadata
    return aggr_df, {
        "join_type": join_type,
        "join_item_name": join_item_name,
        "join_criteria": criteria_list,
        "renamed_columns": [f"{c} → {c}2" for c in common_cols],
    }

 
# Deduplicate columns
def apply_dynamic_dedup(df, dedup_columns_str):
    """
    Drops duplicate rows from a DataFrame based on one or more columns.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        dedup_columns_str (str): Comma-separated list of columns to deduplicate by.
                                 Example: "column1,column2" or "column1"

    Returns:
        DataFrame: Deduplicated DataFrame.
    """

    # Clean and split the string safely
    if not dedup_columns_str or not dedup_columns_str.strip():
        raise ValueError("dedup_columns_str cannot be empty")

    columns = [col.strip() for col in dedup_columns_str.split(",") if col.strip()]

    if dedup_columns_str.strip().lower() == "all_columns":
        deduped_df = df.dropDuplicates()
    else:
        columns = [col.strip() for col in dedup_columns_str.split(",") if col.strip()]

        if not columns:
            raise ValueError(f"No valid columns found in: {dedup_columns_str}")

        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {missing}")

        deduped_df = df.dropDuplicates(columns)

    print(f"✅ Dropped duplicates using columns: {columns}")
    return deduped_df


# Drop columns
def dynamic_column_drop(df, drop_columns_str):
    """
    Drops one or more columns from a PySpark DataFrame.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        drop_columns_str (str): Comma-separated list of columns to drop.
                                Example: "column1,column2" or "column1"

    Returns:
        DataFrame: DataFrame with specified columns dropped.
    """

    # --- Validate input
    if not drop_columns_str or not drop_columns_str.strip():
        raise ValueError("❌ drop_columns_str cannot be empty")

    # --- Clean and split string safely
    drop_list = [c.strip() for c in drop_columns_str.split(",") if c.strip()]

    if not drop_list:
        raise ValueError(f"❌ No valid columns found in: {drop_columns_str}")

    # --- Validate columns exist in the DataFrame
    missing = [c for c in drop_list if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Columns not found in DataFrame: {missing}")

    # --- Drop columns
    reduced_df = df.drop(*drop_list)

    print(f"✅ Dropped columns: {drop_list}")
    return reduced_df


# Convert column to datetime format
def convert_column_to_datetime(df, rule_str):
    """
    Converts a column to Spark date/timestamp.
    Handles 'yyyy-MM' by defaulting to the last day of the month.
    """
    parts = rule_str.split(",")
    if len(parts) < 2:
        raise ValueError(f"Invalid rule format: {rule_str}")

    column_name = parts[0].strip()
    datetime_format = parts[1].strip()

    if datetime_format == "yyyy-MM":
        # Special handling: assign last day of month
        df = df.withColumn(column_name, F.substring(F.col(column_name), 1, 7)).withColumn(
                    "first_day",
                    F.to_date(F.concat(F.col(column_name), F.lit("-01")), "yyyy-MM-dd")
                ).withColumn(
                    "last_day",
                    F.last_day(F.col("first_day"))
                ).withColumn(
                    column_name,
                    F.to_timestamp(F.col("last_day"))
                ).drop("first_day", "last_day")
    else:
        # General case
        @udf(returnType=TimestampType())
        def parse_to_ts(value):
            if value is None:
                return None
            try:
                return parser.parse(value)
            except:
                return None

        df = df.withColumn(
            column_name,
            parse_to_ts(col(column_name))
        )
    display(df)
    return df


# Convert column to int format
def convert_column_to_int(df, convert_columns_str):
    """
    Converts one or more columns in a PySpark DataFrame to IntegerType
    based on a comma-separated list of column names.

    Example:
        convert_column_to_int(df, 'col1,col2')

    Returns:
        DataFrame: Updated DataFrame with converted columns.
    """
    # --- Validate input
    if not convert_columns_str or not convert_columns_str.strip():
        raise ValueError("❌ convert_columns_str cannot be empty")

    # --- Clean and split string safely
    convert_list = [c.strip() for c in convert_columns_str.split(",") if c.strip()]

    if not convert_list:
        raise ValueError(f"❌ No valid columns found in: {convert_columns_str}")

    # --- Validate columns exist in the DataFrame
    missing = [c for c in convert_list if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Columns not found in DataFrame: {missing}")

    # --- Convert each column
    converted_df = df
    for col_name in convert_list:
        converted_df = converted_df.withColumn(col_name, F.col(col_name).cast("int"))

    print(f"✅ Converted the following column(s) to type Int: {convert_list}")
    return converted_df


# Derive a new column from an existing column
def add_derived_column(df, rule_str):
    """
    rule_str example:
        'compliance_control|F.when(F.instr(F.col("metadata_name"), "_") > 0,
                                   F.element_at(F.split(F.col("metadata_name"), "_"), -1))
                              .otherwise(F.col("metadata_name"))'
    """
    parts = rule_str.split("|", 1)
    if len(parts) < 2:
        raise ValueError(f"Invalid rule format: {rule_str}")

    column_name = parts[0].strip()
    derive_column_rule = parts[1].strip()

    # Evaluate the expression safely using the local namespace
    try:
        derived_col_expr = eval(derive_column_rule, {"F": F})
    except Exception as e:
        raise ValueError(f"Error evaluating derive rule: {derive_column_rule}\n{e}")

    # Add new column
    expanded_df = df.withColumn(column_name, derived_col_expr)

    print(f"✅ Column '{column_name}' added using rule:\n{derive_column_rule}")
    return expanded_df


# Rename column
def rename_column(df, rule_str):
    """
    Renames one or more columns in a DataFrame based on a rule string.

    Args:
        df (DataFrame): Input PySpark DataFrame
        rule_str (str): Rename rule, formatted as:
                        'old_column1:new_column1|old_column2:new_column2|old_column3:new_column3'

    Returns:
        DataFrame: DataFrame with renamed columns
    """
    if not rule_str or not rule_str.strip():
        raise ValueError("rule_str cannot be empty")

    # Split multiple rename rules by '|'
    rename_pairs = [pair.strip() for pair in rule_str.split("|") if pair.strip()]

    renamed_df = df
    renamed_columns = []

    for pair in rename_pairs:
        # Each pair must have 'old:new'
        if ":" not in pair:
            raise ValueError(f"Invalid rename rule format: '{pair}'. Expected 'old_column:new_column'.")

        old_col, new_col = [p.strip() for p in pair.split(":", 1)]

        # Validate that the old column exists
        if old_col not in renamed_df.columns:
            raise ValueError(f"Column '{old_col}' not found in DataFrame.")

        renamed_df = renamed_df.withColumnRenamed(old_col, new_col)
        renamed_columns.append(f"{old_col} → {new_col}")

    print(f"✅ Renamed columns: {', '.join(renamed_columns)}")
    return renamed_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Get Key Vault Secrets for connection details (if applicable)

# CELL ********************

# Get Key Vault secrets

def get_required_secret(vault_name: str, secret_name: str) -> str:
    value = mssparkutils.credentials.getSecret(vault_name, secret_name)
    if not value:
        raise ValueError(f"Secret '{secret_name}' was not found or is empty in Key Vault '{vault_name}'.")
    return value

if azure_key_vault and azure_key_vault_secret_client_id_name and azure_key_vault_secret_client_secret_name:
    connection_client_id = get_required_secret(azure_key_vault, azure_key_vault_secret_client_id_name)
    connection_client_secret = get_required_secret(azure_key_vault, azure_key_vault_secret_client_secret_name)
    connection_properties = {
        "user": connection_client_id,
        "password": connection_client_secret,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read source data

# CELL ********************

if source_type == 'sqldb':
    # Define the query to get all the columns for each table
    query = source_item_name
    print(query)

    # Load data from the SQL database
    source_df = (SparkSession.getActiveSession()
        .read
        .jdbc(url=source_connection_url, table=query, properties=connection_properties)
    )
    display(source_df.head(10))
    print("source_df row count:", source_df.count())

elif source_type == 'lakehouse':
    source_df = spark.sql(f"SELECT * FROM {source_item_name}")
    display(source_df.head(10))
    print("source_df row count:", source_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Apply Transformations

# CELL ********************

# Perform transformations if transformation_flag is 1
if transformation_flag == 1:
    # Aggregate data (join)
    if transformation_rule == 'aggregate_data':
        # Resolve transformation rule criteria
        # Format of 'join:inner|<item (table) to join>|value1|value2'
        result_df, join_info = apply_dynamic_join(source_df, transformation_rule_criteria)
        print(join_info)
        display(result_df.head(10))
        print("aggr_df row count:", result_df.count())

    # Dedupe data
    elif transformation_rule == 'deduplication':
        result_df = apply_dynamic_dedup(
            source_df,
            transformation_rule_criteria
        )
        display(result_df.head(10))
        print("deduped_df row count:", result_df.count())
        
    # Drop columns
    elif transformation_rule == 'drop_column':
        result_df = dynamic_column_drop(source_df, transformation_rule_criteria)
        display(result_df.head(10))
        print("reduced_df row count:", result_df.count())

    # Convert single column to datatime format
    elif transformation_rule == 'convert_column_to_datetime':
        result_df = convert_column_to_datetime(source_df, transformation_rule_criteria)
        display(result_df.head(10))
        print("converted_df row count:", result_df.count())

    # Convert one or multiple columns to integer format
    elif transformation_rule == 'convert_column_to_int':
        result_df = convert_column_to_int(source_df, transformation_rule_criteria)
        display(result_df.head(10))
        print("converted_df row count:", result_df.count())

    # Add derived column
    elif transformation_rule == 'add_derived_column':
        result_df = add_derived_column(source_df, transformation_rule_criteria)
        display(result_df.head(10))
        print("converted_df row count:", result_df.count())

    # Rename column
    elif transformation_rule == 'rename_column':
        result_df = rename_column(source_df, transformation_rule_criteria)
        display(result_df.head(10))
        print("converted_df row count:", result_df.count())
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to target

# CELL ********************

# Write to target
print(f"Writing to target item {target_item_name}")
result_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(target_item_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Output to pipeline

# CELL ********************

# calculate rows read
rows_read = source_df.count()

# calculate rows copied or processed
rows_copied = result_df.count()

# build data consistency check result
verification_result = {
    "status": "Passed" if rows_copied > 0 else "Failed",
    "rowCount": rows_copied
}

# wrap into expected Fabric pipeline output structure
output_json = {
    "source_type": source_type,
    "item_name": target_item_name,
    "rowsRead": rows_read,
    "rowsWritten": rows_copied,
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
