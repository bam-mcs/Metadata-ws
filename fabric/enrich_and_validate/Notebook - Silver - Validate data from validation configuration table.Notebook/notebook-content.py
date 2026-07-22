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
# META       "environmentId": "c5693ce9-906b-a5a6-40b6-f73c4f241634",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Set Parameters

# PARAMETERS CELL ********************

validation_id = ''
source_type = ''
source_item_name = ''
target_item_name = ''

validation_category = ''
validation_scope = ''
validation_criteria = ''
validation_enable_flag = ''

process_stage = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import modules

# CELL ********************

import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from collections import Counter
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validate Data

# CELL ********************

# Perform transformations if validation_enable_flag is 1
if validation_enable_flag == 1:
    parts = validation_criteria.split("|")
    if len(parts) < 2:
        raise ValueError(f"Invalid validation criteria format: {validation_criteria}")

    validation_type = parts[0].strip()           # e.g. 'Columns Count Validation'
    validation_rule = parts[1].strip()           # e.g. 'number'
else:
    print("Validation disabled — stopping notebook execution.")
    sys.exit()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Great Expectations Suites

# CELL ********************

import great_expectations as gx
import great_expectations.expectations as gxe

# Initialise the gx context
context = gx.get_context()

# Specify expectation suite name
suite_name = "gxsuite_" + target_item_name
suite = gx.ExpectationSuite(name=suite_name)

#### Validations

## Completeness

if validation_type == "Column Proportion of Non Null Values Between":
    # Proportion of non null values. Min value and max values to be between 0 and 1.
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = float(min_value)
    max_value = float(max_value)
    print(f"Running validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnProportionOfNonNullValuesToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Null Columns":
    # Unique columns in table
    null_columns = [c.strip() for c in validation_rule.split(",") if c.strip()]
    # Iterate
    for null_col in null_columns:
        print(f"Running validation on: {null_col}")
        expectation = gxe.ExpectColumnValuesToBeNull(column=null_col)
        suite.add_expectation(expectation)


elif validation_type == "Not Null Columns":
    # Unique columns in table
    not_null_columns = [c.strip() for c in validation_rule.split(",") if c.strip()]
    # Iterate
    for not_null_col in not_null_columns:
        print(f"Running validation on: {not_null_col}")
        expectation = gxe.ExpectColumnValuesToNotBeNull(column=not_null_col)


## Numeric

elif validation_type == "Column Max Values Between":
    # Column max values between
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = float(min_value)
    max_value = float(max_value)
    print(f"Running validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnMaxToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Mean Values Between":
    # Column mean values between
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = float(min_value)
    max_value = float(max_value)
    print(f"Running validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnMeanToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Median Values Between":
    # Column median values between
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = float(min_value)
    max_value = float(max_value)
    print(f"Running validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnMedianToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Min Values Between":
    # Column min values between
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = float(min_value)
    max_value = float(max_value)
    print(f"Running validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnMinToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Stdev Values Between":
    # Column stdev values between
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = float(min_value)
    max_value = float(max_value)
    print(f"Running validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnStdevToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Sum Values Between":
    # Column sum values between
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = float(min_value)
    max_value = float(max_value)
    print(f"Running validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnSumToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Values Between":
    # Column values between
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = float(min_value)
    max_value = float(max_value)
    print(f"Running validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


## Schema

elif validation_type == "Column Names Exist":
    # Unique columns in table
    existing_columns = [c.strip() for c in validation_rule.split(",") if c.strip()]
    # Iterate
    for existing_col in existing_columns:
        print(f"Running validation on: {existing_col}")
        expectation = gxe.ExpectColumnToExist(column=existing_col)
        suite.add_expectation(expectation)


elif validation_type == "Column Values In Type List":
    # Values types for the specified column. eg. "NUMBER", "STRING"
    col_name, values_set = [
        v.strip() for v in validation_rule.split(";")
    ]
    set = [c.strip() for c in values_set.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {col_name} values to be in type list: {set}")
    expectation = gxe.ExpectColumnDistinctValuesToBeInSet(
        column=col_name,
        type_list=set
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Count Between":
    # Number of columns between
    min_max_count = [c.strip() for c in validation_rule.split(",") if c.strip()]
    expectation = gxe.ExpectTableColumnCountToBeBetween(
        min_value=int(min_max_count[0]),
        max_value=int(min_max_count[1])
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Count To Equal":
    # Number of columns
    try:
        number_column = int(validation_rule)
        print("Casted integer:", number_column)
    except ValueError:
        print("validation_rule is not an integer")
    expectation = gxe.ExpectTableColumnCountToEqual(value=number_column)
    suite.add_expectation(expectation)


elif validation_type == "Column Names Match Ordered List":
    # Names of columns match ordered list
    existing_columns = [c.strip() for c in validation_rule.split(",") if c.strip()]
    expectation = gxe.ExpectTableColumnsToMatchOrderedList(column_list=existing_columns)
    suite.add_expectation(expectation)


elif validation_type == "Column Names Match Set":
    # Names of columns match unordered set
    existing_columns = [c.strip() for c in validation_rule.split(",") if c.strip()]
    expectation = gxe.ExpectTableColumnsToMatchSet(column_set=existing_columns)
    suite.add_expectation(expectation)


## Uniqueness

elif validation_type == "Column Distinct Values To Be In Set":
    # Column distinct values to be in set. Data values can be a subset of the specified set
    col_name, values_set = [
        v.strip() for v in validation_rule.split(";")
    ]
    set = [c.strip() for c in values_set.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {col_name} distinct values to be in set: {set}")
    expectation = gxe.ExpectColumnDistinctValuesToBeInSet(
        column=col_name,
        value_set=set
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Distinct Values To Contain Set":
    # Column distinct values to contain set
    col_name, values_set = [
        v.strip() for v in validation_rule.split(";")
    ]
    set = [c.strip() for c in values_set.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {col_name} distinct values to contain set: {set}")
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(
        column=col_name,
        value_set=set
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Distinct Values To Equal Set":
    # Column distinct values to equal set
    col_name, values_set = [
        v.strip() for v in validation_rule.split(";")
    ]
    set = [c.strip() for c in values_set.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {col_name} distinct values to equal set: {set}")
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(
        column=col_name,
        value_set=set
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Proportion Unique Values Between":
    # Proportion of Unique Values in the specified column. Min/Max value between 0 and 1.
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = float(min_value)
    max_value = float(max_value)
    print(f"Running {validation_type} validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Unique Value Count Between":
    # Number of unique values in the specified column between min and max number
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = int(min_value)
    max_value = int(max_value)
    print(f"Running {validation_type} validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnUniqueValueCountToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Unique Columns":
    # Unique columns in table
    unique_columns = [c.strip() for c in validation_rule.split(",") if c.strip()]
    # Iterate
    for uniq_col in unique_columns:
        print(f"Running validation on: {uniq_col}")
        expectation = gxe.ExpectColumnValuesToBeUnique(column=uniq_col)
        suite.add_expectation(expectation)


elif validation_type == "Compound Columns Values Unique":
    # Unique values in specified columns
    unique_values = [c.strip() for c in validation_rule.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {unique_values}")
    expectation = gxe.ExpectCompoundColumnsToBeUnique(column_list=unique_values)


elif validation_type == "Column Values Unique Within Record":
    # Unique values in specified columns
    unique_values = [c.strip() for c in validation_rule.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {unique_values}")
    expectation = gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(column_list=unique_values)



## Validity

elif validation_type == "Column Most Common Values In Set":
    # Columns values most common in set
    col_name, values_set = [
        v.strip() for v in validation_rule.split(";")
    ]
    set = [c.strip() for c in values_set.split(",") if c.strip()]
    expectation = gxe.ExpectColumnMostCommonValueToBeInSet(
        column=col_name,
        value_set=set
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Pair Values To Be Equal":
    # Column pair values to be equal
    col_name_1, col_name_2 = [
        v.strip() for v in validation_rule.split(",")
    ]
    print(f"Running {validation_type} validation on: {col_name_1} and {col_name_2}")
    expectation = gxe.ExpectColumnPairValuesToBeEqual(
        column_A = col_name_1,
        column_B = col_name_2
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Value Lengths Between":
    # Column value lengths between min and max
    col_name, min_value, max_value = [
        v.strip() for v in validation_rule.split(",")
    ]
    min_value = int(min_value)
    max_value = int(max_value)
    print(f"Running {validation_type} validation on: {col_name} min value: {min_value} and max value: {max_value}")
    expectation = gxe.ExpectColumnValueLengthsToBeBetween(
        column = col_name,
        min_value = min_value,
        max_value = max_value
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Values Length To Equal":
    # Columns values lengths to equal
    # Cater for multiple column/value pairs
    validation_sets = [
        v.strip() for v in validation_rule.split(";")
    ]
    for validation_set in validation_sets:
        col_name, value = [
            c.strip() for c in validation_set.split(",")
        ]
        print(f"Running {validation_type} validation on: {col_name} value: {value}")
        expectation = gxe.ExpectColumnValueLengthsToEqual(
            column=col_name,
            value=value
        )
        suite.add_expectation(expectation)


elif validation_type == "Column Values In Set":
    # Columns values not in set
    col_name, values_set = [
        v.strip() for v in validation_rule.split(";")
    ]
    set = [c.strip() for c in values_set.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {col_name} matching values in set: {set}")
    expectation = gxe.ExpectColumnValuesToBeInSet(
        column=col_name,
        value_set=set
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Values Match Regex":
    # Columns values do not match regex
    col_name, values_regex = [
        v.strip() for v in validation_rule.split(",")
    ]
    print(f"Running {validation_type} validation on: {col_name} matching regex: {values_regex}")
    expectation = gxe.ExpectColumnValuesToMatchRegex(
        column=col_name,
        regex=values_regex
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Values Match Regex List":
    # Columns values do not match regex
    col_name, regex_list = [
        v.strip() for v in validation_rule.split(";")
    ]
    values_regex = [c.strip() for c in regex_list.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {col_name} matching regex list: {values_regex}")
    expectation = gxe.ExpectColumnValuesToMatchRegexList(
        column=col_name,
        regex_list=values_regex,
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Values Not In Set":
    # Columns values not in set
    col_name, values_set = [
        v.strip() for v in validation_rule.split(";")
    ]
    set = [c.strip() for c in values_set.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {col_name} not matching values in set: {set}")
    expectation = gxe.ExpectColumnValuesToNotBeInSet(
        column=col_name,
        value_set=set
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Values Not Match Regex":
    # Columns values do not match regex
    col_name, values_regex = [
        v.strip() for v in validation_rule.split(",")
    ]
    print(f"Running validation on: {col_name} not matching regex: {values_regex}")
    expectation = gxe.ExpectColumnValuesToNotMatchRegex(
        column=col_name,
        regex=values_regex
    )
    suite.add_expectation(expectation)


elif validation_type == "Column Values Not Match Regex List":
    # Columns values do not match regex
    col_name, regex_list = [
        v.strip() for v in validation_rule.split(";")
    ]
    values_regex = [c.strip() for c in regex_list.split(",") if c.strip()]
    print(f"Running {validation_type} validation on: {col_name} not matching regex list: {values_regex}")
    expectation = gxe.ExpectColumnValuesToNotMatchRegexList(
        column=col_name,
        regex_list=values_regex,
    )
    suite.add_expectation(expectation)


## Volume

elif validation_type == "Row Count Between":
    # Number of columns between
    min_max_count = [c.strip() for c in validation_rule.split(",") if c.strip()]
    expectation = gxe.ExpectTableRowCountToBeBetween(
        min_value=int(min_max_count[0]),
        max_value=int(min_max_count[1])
    )
    suite.add_expectation(expectation)


elif validation_type == "Row Count To Equal":
    # Number of columns
    try:
        number_column = int(validation_rule)
        print("Casted integer:", number_column)
    except ValueError:
        print("validation_rule is not an integer")
    expectation = gxe.ExpectTableRowCountToEqual(value=number_column)
    suite.add_expectation(expectation)

expectation_suite = context.suites.add(suite)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validate dataframe with Great Expectations

# CELL ********************

# declare a spark source as a datasource
data_source_name = target_item_name + "_datasource"
data_source = context.data_sources.add_spark(data_source_name)

# declare a data asset
data_asset_name = target_item_name + "_data_asset"
data_asset = data_source.add_dataframe_asset(name=data_asset_name)

# prepare a Spark Dataframe
validation_dataframe = spark.sql("SELECT * FROM " + target_item_name)

# build batch request passing in the dataframe
batch_definition_name = target_item_name + "_batch_def"
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

# create a validation definition
definition_name = target_item_name + "_validation_def"
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=expectation_suite, name=definition_name
)

# Test the Expectation
batch_parameters_dataframe = {"dataframe": validation_dataframe}
validation_results = validation_definition.run(batch_parameters=batch_parameters_dataframe)
print(validation_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Output to pipeline

# CELL ********************

# wrap into expected Fabric pipeline output structure
if source_type == "azure_resource_graph":
    source_item_name = source_type

validation_results_dict = validation_results.to_json_dict()
validation_status = "Passed" if validation_results_dict["success"] else "Failed"

output_json = {
    "validation_id": validation_id,
    "source_type": source_type,
    "source_item_name": source_item_name,
    "target_item_name": target_item_name,
    "process_stage": process_stage,
    "validation_category": validation_category,
    "validation_scope": validation_scope,
    "validation_criteria": validation_criteria,
    "validation_status": validation_status
}

# return structured JSON to pipeline
mssparkutils.notebook.exit(json.dumps(output_json))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
