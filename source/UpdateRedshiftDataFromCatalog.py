import sys
from typing import List
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import DecimalType, LongType
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame
from utility import (
    prepare_dictionaries_from_blended_parameter,
    get_partition_config_for_table,
    log_output,
    cast_decimal_to_long,
)


def parse_table_spec(spec: str) -> List[dict]:
    """Parse table specification string into list of table configs
    Format: table1:*:40000;table2;table3:col1,col2,col3:900000
    Returns: [{"name": "table1", "columns": ["col1","col2"], "last_id": 1000}, ...]
    """
    tables = []
    for table_spec in spec.strip().split(";"):
        parts = table_spec.split(":")
        table_config = {
            "name": parts[0],
            "columns": ["*"],  # default to all columns
            "last_id": 0,  # default to 0
        }
        # If columns are specified
        if len(parts) >= 2 and parts[1]:  # Check if not empty
            table_config["columns"] = parts[1].split(",") if parts[1] != "*" else ["*"]
        # If last_id is specified
        if len(parts) >= 3 and parts[2]:  # Check if not empty
            table_config["last_id"] = int(parts[2])

        tables.append(table_config)
    return tables


class UpdateRedshiftDataFromCatalog:
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
            "S3_TEMP_DIR",
            "TABLES",
            "PARTITIONS",
        ]
        args = getResolvedOptions(sys.argv, params)
        self.environment = (
            "PRODUCTION" if "RUNTIME_ENV" not in args else args["RUNTIME_ENV"].upper()
        )

        # Parse table specifications
        self.tables = prepare_dictionaries_from_blended_parameter(
            args["TABLES"], keys=["name", "columns", "last_id"]
        )
        self.source_db = args["SOURCE_DB"]
        self.source_table_prefix = args["SOURCE_TABLE_PREFIX"]
        self.destination_connection = args["DESTINATION_CONNECTION"]
        self.destination_db = args["DESTINATION_DB"]
        self.destination_schema = args["DESTINATION_SCHEMA"]
        self.s3_temp_dir = args["S3_TEMP_DIR"]
        self.partition_configs = prepare_dictionaries_from_blended_parameter(
            args["PARTITIONS"], keys=["table_name", "hashfield", "hashpartitions"]
        )

        log_output(f"Parsed tables: {self.tables}")
        log_output(
            f"Source DB: {self.source_db}, Source Table Prefix: {self.source_table_prefix}"
        )

        self.job_name = args["JOB_NAME"]

        return

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

    def create_dynamic_frame_from_catalog(self, table_name, columns, last_id):
        # Default to ["*"] if columns is None or empty
        catalog_table_name = self.source_table_prefix + table_name
        columns = columns if columns else ["*"]
        last_id = last_id if last_id and last_id > 0 else 0

        # Get partitions configurations for table
        partition_config = get_partition_config_for_table(
            self.partition_configs, table_name
        )
        hashpartitions, hashfield, should_partition = (
            partition_config["hashpartitions"],
            partition_config["hashfield"],
            partition_config["should_partition"],
        )

        # hashpartitions = "11"
        # hashfield = "user_id"

        additional_options = {
            "enablePartitioningForSampleQuery": False,
            "hashpartitions": "1",
        }

        # Modify columns and offset using sampleQuery
        if columns != ["*"] or last_id > 0:
            # Either custom columns or last_id is specified
            sample_query_datastore = f"SELECT {','.join(columns)} FROM {table_name}"

            if last_id > 0:
                # last_id is specified
                sample_query_datastore += f" WHERE id>={last_id}"

                if should_partition:
                    # Partitioning is enabled
                    sample_query_datastore += f" AND"
                    additional_options["enablePartitioningForSampleQuery"] = True
            elif should_partition:
                # Partitioning is enabled but last_id is not specified
                sample_query_datastore += f" WHERE"
                additional_options["enablePartitioningForSampleQuery"] = True

            additional_options["sampleQuery"] = sample_query_datastore

        # Configure partitioning
        if should_partition:
            additional_options.update(
                {
                    "hashpartitions": hashpartitions,
                    "hashfield": hashfield,
                    # "hashexpression": partition_config["hashfield"],
                }
            )

        log_output(
            f"Reading from Catalog: DB: {self.source_db}, "
            f"Table: {self.source_table_prefix + table_name}, "
            f"last_id: {last_id}, columns: {columns} "
            f"Additional Options: {additional_options}"
        )

        if self.environment != "PROD":
            return

        # Use Glue Catalog connection
        dynamic_frame = self.context.create_dynamic_frame.from_catalog(
            database=self.source_db,
            table_name=catalog_table_name,
            additional_options=additional_options,
        )

        # dynamic_frame.printSchema()
        # log_output(f"Number of rows read: {dynamic_frame.count()}")
        # dynamic_frame.toDF().show(5)
        return dynamic_frame

    def write_to_redshift_using_connection(self, dynamic_frame, table_name, last_id):
        if self.environment != "PROD":
            return

        # If last_id > 0, delete rows with id > last_id, otherwise truncate the table
        if last_id and last_id > 0:
            preaction = f"DELETE FROM {self.destination_schema}.{table_name} WHERE id > {last_id};"
        else:
            preaction = f"TRUNCATE TABLE {self.destination_schema}.{table_name};"

        connection_options = {
            "redshiftTmpDir": self.s3_temp_dir,
            "useConnectionProperties": "true",
            "dbtable": f"{self.destination_schema}.{table_name}",
            "connectionName": self.destination_connection,
            "preactions": preaction,
            "extracopyoptions": "TRUNCATECOLUMNS MAXERROR 1",
        }

        log_output(
            f"Writing to Redshift table: {table_name}, options: {connection_options}"
        )

        self.context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options=connection_options,
            # transformation_ctx=f"redshift_write_{table_name}",
        )
        log_output(f"Successfully wrote to Redshift table: {table_name}")

    def run(self):
        try:
            for table_config in self.tables:
                dynamic_frame = self.create_dynamic_frame_from_catalog(
                    table_name=table_config["name"],
                    columns=table_config["columns"],
                    last_id=table_config["last_id"],
                )

                dynamic_frame = cast_decimal_to_long(self.context, dynamic_frame)

                self.write_to_redshift_using_connection(
                    dynamic_frame,
                    table_name=table_config["name"],
                    last_id=table_config["last_id"],
                )
            self.commit_job()
        except Exception as e:
            self.commit_job()
            raise e


if __name__ == "__main__":
    UpdateRedshiftDataFromCatalog().run()
