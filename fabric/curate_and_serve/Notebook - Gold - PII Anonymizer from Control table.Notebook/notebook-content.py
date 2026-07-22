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
# META       "default_lakehouse_workspace_id": "62cb089d-0592-41c9-a0c8-f658d407f812",
# META       "known_lakehouses": [
# META         {
# META           "id": "7ce8f207-9318-4e04-bb82-ba0f80714e84"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "4d306e3a-30c4-966b-4d68-06261997b9be",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Set Parameters

# PARAMETERS CELL ********************

source_type = ''
source_item_name = ''

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

# ## Import Modules

# CELL ********************

from pyspark.sql.functions import (
    array, lit, explode, col, monotonically_increasing_id, concat
)
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from presidio_anonymizer.entities import OperatorConfig
import pandas as pd
import json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Select spacy model in configuration
# 
# ### en_core_web_md = medium model
# https://spacy.io/models/en#en_core_web_md
# ### en_core_web_lg = large model
# https://spacy.io/models/en#en_core_web_lg

# CELL ********************

configuration = {
    "nlp_engine_name": "spacy",
    "models": [
        {"lang_code": "en", "model_name": "en_core_web_md"},
    ]
}

provider = NlpEngineProvider(nlp_configuration=configuration)
nlp_engine = provider.create_engine()

analyzer = AnalyzerEngine(
    nlp_engine=nlp_engine, supported_languages=["en"]
)
anonymizer = AnonymizerEngine()

# Broadcasting analyzer and anonymizer objects in Spark to serialize the object only once.
# Send it to each worker node once, and reuse it for all tasks on that node.
broadcasted_analyzer = spark.sparkContext.broadcast(analyzer)
broadcasted_anonymizer = spark.sparkContext.broadcast(anonymizer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Functions

# CELL ********************

# Identify and Redact PII column
def identify_and_redact_pii_column(df, rule_str):
    """
    Parses and applies a redaction on a column based on the rule string.
    If redacted column name is specify as 'none', the default <column_name>_redacted will be used.
    Example rule:
        '<column name>|<replacement text>|<redacted column name>'
        'email_address|none|none'
    
    Returns:
      redacted_df
    """
    # Split into sections
    parts = rule_str.split("|")
    if len(parts) < 3:
        raise ValueError(f"Invalid rule format: {rule_str}")

    column_name = parts[0].strip()
    replacement_txt = parts[1].strip()
    output_column_name = parts[2].strip()

    # Set replacement text to empty string if 'none' is specified
    if replacement_txt == "none":
        replacement_txt = ""

    # New column for always generated. The redacted column name is used to specify a custom name other than the default
    if output_column_name is None:
        output_column_name = f"{column_name}_redacted"
    return df.withColumn(
        output_column_name,
        identify_and_redact_pii_pandas_udf(df[column_name], lit(replacement_txt))
    )

@pandas_udf(StringType())
def identify_and_redact_pii_pandas_udf(texts: pd.Series, replacements: pd.Series) -> pd.Series:
    analyzer = broadcasted_analyzer.value
    anonymizer = broadcasted_anonymizer.value

    def redact(text, replacement):
        if text is None:
            return None
        results = analyzer.analyze(text=text, entities=[], language='en')
        operators = {"DEFAULT": OperatorConfig("replace", {"new_value": replacement})}
        anonymized_result = anonymizer.anonymize(
            text=text,
            analyzer_results=results,
            operators=operators
        )
        return anonymized_result.text

    return pd.Series([redact(t, r) for t, r in zip(texts, replacements)])


# Partial mask column
def partial_mask_column(df, rule_str):
    """
    Parses and applies a partial mask on a column based on the rule string.
    If masked column name is specify as 'none', the default <column_name>_masked will be used.
    Example rule:
        '<column name>|<output column>|<mask char>|<mask from first>|<num chars>'
        'email_address|none|*|True|0'
    
    Returns:
      redacted_df
    """
    parts = rule_str.split("|")
    if len(parts) < 5:
        raise ValueError(f"Invalid rule format: {rule_str}")

    column_name = parts[0].strip()
    output_column = parts[1].strip()
    mask_char = parts[2].strip()
    mask_from_first = parts[3].strip()
    num_chars = parts[4].strip()

    if output_column == "none":
        output_column = f"{column_name}_masked"

    return df.withColumn(
        output_column,
        partial_mask_pandas_udf(
            col(column_name),
            lit(mask_char),
            lit(mask_from_first).cast("boolean"),
            lit(num_chars).cast("int")   
        )
    )

@pandas_udf(StringType())
def partial_mask_pandas_udf(value: pd.Series, mask_char: pd.Series, mask_from_first: pd.Series, num_chars: pd.Series) -> pd.Series:
    def mask_single(v, m, mf, n):
        if pd.isnull(v):
            return None
        v = str(v)
        length = len(v)
        num_to_mask = min(n, length)
        if mf:
            return m * num_to_mask + v[num_to_mask:]
        else:
            return v[:-num_to_mask] + m * num_to_mask
    return pd.Series([mask_single(v, m, mf, n) for v, m, mf, n in zip(value, mask_char, mask_from_first, num_chars)])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read source data

# CELL ********************

if source_type == 'lakehouse':
    source_df = spark.sql(f"SELECT * FROM {source_item_name}")
    display(source_df.head(10))
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Select PII anonymizer

# CELL ********************

if transformation_flag == 1:
    if transformation_rule == 'pii_redact_column':
        result_df = identify_and_redact_pii_column(source_df, transformation_rule_criteria)
        display(result_df.head(10))

    if transformation_rule == 'pii_partial_mask_column':
        result_df = partial_mask_column(source_df, transformation_rule_criteria)
        display(result_df.head(10))


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
