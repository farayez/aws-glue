import sys
from typing import List
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


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


def log_output(message: str):
    print(f"[INFO] {message}")


def map_spark_type_to_redshift(spark_type):
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
    return mapping.get(spark_type.lower(), "VARCHAR(65535)")


def generate_redshift_ddl_from_schema(schema, table_name, schema_name):
    cols = [
        f'"{field.name}" {map_spark_type_to_redshift(field.dataType.typeName())}'
        for field in schema.fields
    ]
    return f'CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n  {",  ".join(cols)}\n);'


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

    def read_from_rds(self, table_config):
        catalog_table_name = self.source_table_prefix + table_config["name"]
        sample_query_datastore = f"SELECT {','.join(table_config['columns'])} FROM {table_config['name']} WHERE id>={table_config['last_id']}"

        log_output(
            f"Reading from Catalog: DB: {self.source_db}, "
            f"Table: {self.source_table_prefix + table_config['name']}, "
            f"last_id: {table_config['last_id']}, columns: {table_config['columns']} "
            f"Sample Query: {sample_query_datastore}"
        )

        if self.environment != "PROD":
            return

        # Use Glue Catalog connection
        df = self.context.create_dynamic_frame.from_catalog(
            database=self.source_db,
            table_name=catalog_table_name,
            additional_options={
                "sampleQuery": sample_query_datastore,
            },
        )

        # df.printSchema()
        # log_output(f"Number of rows read: {df.count()}")
        # df.toDF().show(5)
        return df

    def write_to_redshift_using_connection(self, dynamic_frame, table_name):
        if self.environment != "PROD":
            return
        # return

        create_table_sql = generate_redshift_ddl_from_schema(
            dynamic_frame.schema(), table_name, self.destination_schema
        )

        connection_options = {
            "redshiftTmpDir": self.s3_temp_dir,
            "useConnectionProperties": "true",
            "dbtable": f"{self.destination_schema}.{table_name}",
            "connectionName": self.destination_connection,
            "preactions": create_table_sql,
            "extracopyoptions": "TRUNCATECOLUMNS MAXERROR 1",
            # "postactions": f"ANALYZE {redshift_schema}.{table};",
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

    def write_to_redshift_using_jdbc_conf(self, dynamic_frame, table_name):
        if self.environment != "PROD":
            return
        # return

        connection_options = {
            "dbtable": f"{self.destination_schema}.{table_name}",
            "database": self.destination_db,
            # "aws_iam_role": "arn:aws:iam::role-account-id:role/rs-role-name",
            "redshiftTmpDir": self.s3_temp_dir,
        }

        # Use Redshift connection options
        self.context.write_dynamic_frame.from_jdbc_conf(
            frame=dynamic_frame,
            catalog_connection=self.destination_connection,
            redshift_tmp_dir=self.s3_temp_dir,
            connection_options=connection_options,
        )

    def run(self):
        try:
            for table_config in self.tables:
                df = self.read_from_rds(table_config)
                self.write_to_redshift_using_connection(df, table_config["name"])
            self.commit_job()
        except Exception as e:
            self.commit_job()
            raise e


if __name__ == "__main__":
    GlueRDSToRedshift().run()
