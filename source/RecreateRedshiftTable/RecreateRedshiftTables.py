import sys
from typing import List
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

import boto3
from utility import (
    parse_table_spec_from_blended_parameter,
    log_output,
    get_glue_catalog_schema,
    recreate_redshift_table_from_columns,
)


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
        self.tables = parse_table_spec_from_blended_parameter(
            args["TABLES"], keys=["name", "columns", "sort_key", "dist_key"]
        )
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

        return

    def init_context(self, job_name):
        if self.environment != "PROD":
            return

        self.glue_client = boto3.client("glue")
        self.redshift_client = boto3.client("redshift-data")
        self.context = GlueContext(SparkContext.getOrCreate())
        self.job = Job(self.context)
        self.job.init(job_name)

    def commit_job(self):
        if self.environment != "PROD":
            return
        self.job.commit()

    def run(self):
        try:
            for table_config in self.tables:
                schema = get_glue_catalog_schema(
                    self.glue_client,
                    self.source_db,
                    self.source_table_prefix + table_config["name"],
                    table_config["columns"],
                )
                log_output(f"Schema for {table_config['name']}: {schema}")
                recreate_redshift_table_from_columns(
                    self.redshift_client,
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
