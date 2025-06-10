import sys
from typing import List
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

import boto3


def parse_table_spec(spec: str) -> List[dict]:
    """Parse table specification string into list of table configs
    Format: table1:*:40000;table2;table3:col1,col2,col3:col2:col3
    Returns: [{"name": "table1", "columns": ["col1","col2"], "sort_key": "col2", "dist_key": "col3"}, ...]
    """
    tables = []
    for table_spec in spec.strip().split(";"):
        parts = table_spec.split(":")
        table_config = {
            "name": parts[0],
            "columns": ["*"],  # default to all columns
            "sort_key": None,
            "dist_key": None,
        }
        # If columns are specified
        if len(parts) >= 2 and parts[1]:  # Check if not empty
            table_config["columns"] = parts[1].split(",") if parts[1] != "*" else ["*"]
        # If sort_key is specified
        if len(parts) >= 3 and parts[2]:  # Check if not empty
            table_config["sort_key"] = parts[2]
        # If dist_key is specified
        if len(parts) >= 4 and parts[3]:  # Check if not empty
            table_config["dist_key"] = parts[3]

        tables.append(table_config)
    return tables


def log_output(message: str):
    print(f"[INFO] {message}")


def map_glue_type_to_redshift(glue_type):
    mapping = {
        "string": "VARCHAR(65535)",
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


def recreate_redshift_table_from_columns(
    workgroup_name,
    database,
    destination_schema,
    table_config,
    redshift_columns,
):
    """Drop and recreate a Redshift table using boto3 Redshift Data API based on Redshift column definitions."""

    client = boto3.client("redshift-data")
    table_name = table_config["name"]
    sort_key = table_config["sort_key"]
    dist_key = table_config["dist_key"]

    create_table_stmt = f"""
    DROP TABLE IF EXISTS {destination_schema}.{table_name};
    CREATE TABLE {destination_schema}.{table_name} ({', '.join(redshift_columns)})
    """
    # Add sort and distribution keys if specified
    if sort_key:
        create_table_stmt += f" SORTKEY ({sort_key})"
    if dist_key:
        create_table_stmt += f" DISTKEY ({dist_key})"

    create_table_stmt += f";ANALYZE {destination_schema}.{table_name};"

    print(f"[INFO] Executing DDL:\n{create_table_stmt}")

    response = client.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=create_table_stmt,
    )

    print(f"[INFO] Redshift DDL execution response: {response}")
    return response


def get_glue_catalog_schema(database_name, table_name, included_columns=None):
    """Fetch the table schema from Glue Data Catalog and convert to Spark StructType."""

    glue_client = boto3.client("glue")

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

    print(f"[INFO] Fetched Redshift schema for {database_name}.{table_name}:")
    for col_def in redshift_columns:
        print(col_def)

    return redshift_columns


class GlueRDSToRedshift:
    def __init__(self):
        self.parse_arguments()

        # Initialize Glue context and job
        self.init_context(self.job_name)

    def parse_arguments(self):
        params = [
            "JOB_NAME",
            "RUNTIME_ENV",
            "SOURCE_DB",
            "SOURCE_TABLE_PREFIX",
            "DESTINATION_CONNECTION",
            "DESTINATION_DB",
            "DESTINATION_SCHEMA",
            "DESTINATION_WORKGROUP_NAME",
            "S3_TEMP_DIR",
            "TABLES",
        ]
        args = getResolvedOptions(sys.argv, params)
        self.environment = (
            "PRODUCTION" if "RUNTIME_ENV" not in args else args["RUNTIME_ENV"].upper()
        )

        # Parse table specifications
        self.tables = parse_table_spec(args["TABLES"])
        self.source_db = args["SOURCE_DB"]
        self.source_table_prefix = args["SOURCE_TABLE_PREFIX"]
        self.destination_connection = args["DESTINATION_CONNECTION"]
        self.destination_db = args["DESTINATION_DB"]
        self.destination_schema = args["DESTINATION_SCHEMA"]
        self.destination_workgroup_name = args["DESTINATION_WORKGROUP_NAME"]
        self.s3_temp_dir = args["S3_TEMP_DIR"]
        log_output(f"Parsed tables: {self.tables}")
        log_output(
            f"Source DB: {self.source_db}, Source Table Prefix: {self.source_table_prefix}"
        )

        self.job_name = args["JOB_NAME"]

    def init_context(self, job_name):
        if self.environment != "PROD":
            return

        self.context = GlueContext(SparkContext.getOrCreate())
        self.job = Job(self.context)
        self.job.init(job_name)

    def commit_job(self):
        if self.environment != "PROD":
            return
        self.job.commit()

    def run(self):
        log_output(f"self.tables: {self.tables}")
        try:
            for table_config in self.tables:
                schema = get_glue_catalog_schema(
                    self.source_db,
                    self.source_table_prefix + table_config["name"],
                    table_config["columns"],
                )
                log_output(f"Schema for {table_config['name']}: {schema}")
                recreate_redshift_table_from_columns(
                    self.destination_workgroup_name,
                    self.destination_db,
                    self.destination_schema,
                    table_config,
                    schema,
                )
            self.commit_job()
        except Exception as e:
            self.commit_job()
            raise e


if __name__ == "__main__":
    GlueRDSToRedshift().run()
