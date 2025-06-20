from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    TimestampType,
    DateType,
    DecimalType,
    LongType,
)
from typing import List, Optional, Type
import boto3
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col


def map_glue_type_to_redshift(glue_type):
    mapping = {
        "string": "VARCHAR(255)",
        "int": "INTEGER",
        "bigint": "BIGINT",
        "double": "DOUBLE PRECISION",
        "float": "FLOAT4",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "decimal": "DECIMAL(38,10)",
    }
    return mapping.get(glue_type.lower(), "VARCHAR(65535)")


def generate_redshift_create_table_stmnt(schema, table_name, schema_name):
    cols = [
        f'"{field.name}" {map_glue_type_to_redshift(field.dataType.typeName())}'
        for field in schema.fields
    ]
    return (
        f'CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (  {",  ".join(cols)});'
    )


def map_glue_type_to_spark(glue_type):
    """Map Glue column types to Spark data types."""
    glue_type = glue_type.lower()
    if glue_type in ["string", "varchar", "char"]:
        return StringType()
    elif glue_type in ["int", "integer"]:
        return IntegerType()
    elif glue_type == "bigint":
        return IntegerType()
    elif glue_type in ["float", "double"]:
        return DoubleType()
    elif glue_type == "boolean":
        return BooleanType()
    elif glue_type == "timestamp":
        return TimestampType()
    elif glue_type == "date":
        return DateType()
    elif glue_type.startswith("decimal"):
        # Extract precision and scale if present
        import re

        match = re.match(r"decimal\((\d+),\s*(\d+)\)", glue_type)
        if match:
            precision, scale = int(match.group(1)), int(match.group(2))
            return DecimalType(precision, scale)
        else:
            return DecimalType(38, 10)
    else:
        return StringType()  # default fallback


def get_redshift_columns_from_catalog(
    glue_client, database_name, table_name, included_columns=None
):
    """Fetch the table schema from Glue Data Catalog and convert to Redshift Types."""

    if not glue_client:
        glue_client = boto3.client("glue")  # type: ignore

    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    columns = response["Table"]["StorageDescriptor"]["Columns"]

    # If included_columns is None or contains '*', use all columns
    include_all = not included_columns or "*" in included_columns

    redshift_columns = []
    for col in columns:
        col_name = col["Name"]
        if include_all or col_name in included_columns:
            glue_type = col["Type"]
            redshift_type = map_glue_type_to_redshift(glue_type)
            redshift_columns.append(f'"{col_name}" {redshift_type}')

    return redshift_columns


def cast_decimal_to_long(glue_context, dynamic_frame):
    """Cast Decimal(20,0) to Long in the DynamicFrame."""
    df = dynamic_frame.toDF()
    for field in df.schema.fields:
        if (
            isinstance(field.dataType, DecimalType)
            and field.dataType.scale == 0
            and field.dataType.precision == 20
        ):
            log_output(f"Casting column {field.name} from Decimal(20,0) to Long")
            df = df.withColumn(field.name, col(field.name).cast(LongType()))

    return DynamicFrame.fromDF(df, glue_context, "casted_df")


def recreate_redshift_table_from_columns(
    redshift_client,
    workgroup_name,
    database,
    destination_schema,
    table_config,
    redshift_columns,
):
    """Drop and recreate a Redshift table using boto3 Redshift Data API based on Redshift column definitions."""

    if not redshift_client:
        redshift_client = boto3.client("redshift-data")  # type: ignore

    table_name = table_config["name"]
    sort_key = table_config["sort_key"]
    dist_key = table_config["dist_key"]

    create_table_stmt = f"DROP TABLE IF EXISTS {destination_schema}.{table_name}; CREATE TABLE {destination_schema}.{table_name} ({', '.join(redshift_columns)})"

    # Add sort and distribution keys if specified
    if sort_key:
        create_table_stmt += f" SORTKEY ({sort_key})"
    # if dist_key:
    #     create_table_stmt += f" DISTKEY ({dist_key})"

    create_table_stmt += f"; ANALYZE {destination_schema}.{table_name};"

    log_output(f"Executing DDL:\n{create_table_stmt}")

    response = redshift_client.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=create_table_stmt,
    )

    log_output(f"Redshift DDL execution response: {response}")
    return response


def log_output(message: str):
    print(f"[INFO] {message}")


def prepare_dictionaries_from_blended_parameter(
    blended_parameter: str,
    keys: List[str],
    types: Optional[List[Type]] = None,
) -> List[dict]:
    """Convert blended parameter string into a list of dictionaries using provided keys.

    Args:
        blended_parameter (str): String in format "val1:val2:val3;val4:val5:val6"
        keys (List[str]): List of keys to use for dictionary creation.
        types (List[type], optional): List of types to convert values to. Must match length of keys.
                                    Defaults to None (no type conversion).

    Returns:
        List[dict]: List of dictionaries where each value from blended parameter is mapped to corresponding key
                   and converted to specified type if types parameter is provided.

    Example:
        >>> prepare_dictionaries_from_blended_parameter(
        ...     "table1:10:key1;table2:20:key2",
        ...     ["name", "id", "sort_key"],
        ...     [str, int, str]
        ... )
        [{"name": "table1", "id": 10, "sort_key": "key1"},
         {"name": "table2", "id": 20, "sort_key": "key2"}]
    """
    if types and len(types) != len(keys):
        raise ValueError("Length of types must match length of keys")

    blended_config = parse_blended_parameter(
        blended_parameter, entry_delimiter=";", value_delimiter=":"
    )

    result = []
    for values in blended_config:
        config: dict = {}
        for i, key in enumerate(keys):
            value = values[i] if i < len(values) else None

            # Apply type conversion if types is provided and value exists
            if types and value is not None:
                try:
                    # Special handling for list type (comma-separated strings)
                    if types[i] == list and isinstance(value, str):
                        value = [v.strip() for v in value.split(",") if v.strip()]
                    else:
                        value = types[i](value.strip())
                except (ValueError, TypeError) as e:
                    raise ValueError(
                        f"Failed to convert value '{value}' to type {types[i]} for key '{key}': {str(e)}"
                    )

            config[key] = value

        result.append(config)

    return result


def prepare_dictionaries_from_blended_parameter2(
    blended_parameter: str,
    keys: List[str],
) -> List[dict]:
    """Convert blended parameter string into a list of dictionaries using provided keys.

    Args:
        blended_parameter (str): String in format "val1:val2:val3;val4:val5:val6"
        keys (List[str], optional): List of keys to use for dictionary creation.
                                  Defaults to ["name", "columns", "sort_key", "dist_key"].

    Returns:
        List[dict]: List of dictionaries where each value from blended parameter is mapped to corresponding key.
                   If a value is None or there are fewer values than keys, those keys will have None value.

    Example:
        >>> prepare_dictionaries_from_blended_parameter("table1:col1,col2:key1;table2:*:key2",
        ...                                       ["name", "columns", "sort_key"])
        [{"name": "table1", "columns": ["col1","col2"], "sort_key": "key1"},
         {"name": "table2", "columns": ["*"], "sort_key": "key2"}]
    """
    blended_config = parse_blended_parameter(
        blended_parameter, entry_delimiter=";", value_delimiter=":"
    )

    result = []
    for values in blended_config:
        # Create dictionary by zipping keys with values
        # If values list is shorter than keys list, remaining keys will get None values
        config: dict = {}
        for i, key in enumerate(keys):
            config[key] = values[i] if i < len(values) else None

        # Handle special case for columns field - if it's a comma-separated string, split it into list
        if config.get("columns") is not None:
            config["columns"] = [
                col.strip() for col in config["columns"].split(",") if col.strip()
            ]

        # Convert numeric fields to integers
        if config.get("start_id") is not None:
            config["start_id"] = int(config["start_id"].strip())

        if config.get("end_id") is not None:
            config["end_id"] = int(config["end_id"].strip())

        result.append(config)

    return result


def parse_blended_parameter(
    parameter: str, entry_delimiter: str = ";", value_delimiter: str = ":"
) -> List[List[str]]:
    """Parse a parameter string into a list of value lists using specified delimiters.

    Args:
        parameter (str): The parameter string to parse
        entry_delimiter (str, optional): The delimiter separating different entries. Defaults to ";".
        value_delimiter (str, optional): The delimiter separating values within an entry. Defaults to ":".

    Returns:
        List[List[str]]: A list of lists, where each inner list contains the values for one entry.
                        Empty or missing values are represented as None.

    Example:
        >>> parse_blended_parameter("a:b:c;d:e", ";", ":")
        [['a', 'b', 'c'], ['d', 'e', None]]
    """
    if not parameter or not parameter.strip():
        return []

    blended_config = []
    for spec in parameter.strip().split(entry_delimiter):
        if not spec.strip():
            continue
        parts = [p.strip() or None for p in spec.split(value_delimiter)]
        blended_config.append(parts)
    return blended_config


def get_partition_config_for_table(partition_configs, table_name):
    # Find matching partition config for the table
    partition_config = next(
        (config for config in partition_configs if config["table_name"] == table_name),
        {},
    )

    hashpartitions = (
        partition_config["hashpartitions"]
        if partition_config.get("hashpartitions")
        else "1"
    )
    hashfield = (
        partition_config["hashfield"] if partition_config.get("hashfield") else "id"
    )
    should_partition = hashpartitions and int(hashpartitions) > 1

    return {
        "should_partition": should_partition,
        "hashpartitions": hashpartitions,
        "hashfield": hashfield,
    }
