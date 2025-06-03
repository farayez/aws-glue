"""
AWS Glue ETL job for incremental data migration from MySQL to Amazon Redshift.

This script provides an automated, fault-tolerant process for copying data from a MySQL database
to Amazon Redshift. It supports incremental loading using bookmark IDs and handles:
- Automatic schema detection and creation in Redshift
- Data type mapping and conversion between MySQL and Redshift
- Decimal to double conversion to prevent precision issues
- Incremental loading with bookmark support to handle large tables
- Automatic table creation if target doesn't exist
- Safe data updates with pre-delete of overlapping records

Required job parameters:
- REDSHIFT_CONNECTION: Name of the Glue connection for Redshift
- REDSHIFT_SCHEMA: Target schema name in Redshift
- S3_TEMP_DIR: S3 location for temporary data storage during transfer
- SOURCE_JDBC_CREDENTIAL: AWS Secrets Manager secret name/ARN containing MySQL credentials
- SOURCE_JDBC_URL: JDBC connection URL for the source MySQL database
- TABLES: Comma-separated list of tables with optional bookmark IDs for incremental loading

Example usage:
    TABLES=orders:150000,customers:500000,products

    This will:
    - Copy orders table records with id > 150000
    - Copy customers table records with id > 500000
    - Copy all records from products table (bookmark = 0)

Note: Ensure appropriate permissions are set up for:
- AWS Secrets Manager access
- S3 temporary bucket access
- Source MySQL database access
- Target Redshift cluster access
"""

import sys
import boto3
import json
import re
from typing import Dict, List
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType

# Initialize logging
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(handler)


def sanitize_identifier(identifier: str) -> str:
    """Sanitize SQL identifiers to prevent SQL injection."""
    if not re.match("^[a-zA-Z0-9_]+$", identifier):
        raise ValueError(
            f"Invalid identifier: {identifier}. Only alphanumeric characters and underscores allowed."
        )
    return identifier


def validate_table_exists(
    glueContext: GlueContext, table: str, connection_options: Dict
) -> bool:
    """Check if a table exists in the source database."""
    try:
        test_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="jdbc",
            connection_options={
                **connection_options,
                "query": f"SELECT 1 FROM {table} LIMIT 1",
            },
        )
        return test_frame.count() >= 0
    except Exception as e:
        logger.warning(f"Table existence check failed for {table}: {str(e)}")
        return False


def get_db_credentials(secret_name: str, region_name: str = "eu-west-1") -> Dict:
    """Fetch database credentials from AWS Secrets Manager."""
    secrets_client = boto3.client("secretsmanager", region_name=region_name)
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response["SecretString"])
        return secret_dict
    except Exception as e:
        print(f"Error fetching secret {secret_name}: {str(e)}")
        raise e


def validate_config(args: Dict) -> None:
    """Validate job configuration parameters."""
    required_params = [
        "REDSHIFT_CONNECTION",
        "REDSHIFT_SCHEMA",
        "S3_TEMP_DIR",
        "SOURCE_JDBC_CREDENTIAL",
        "SOURCE_JDBC_URL",
        "TABLES",
    ]

    missing_params = [param for param in required_params if not args.get(param)]
    if missing_params:
        raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")

    if not args["S3_TEMP_DIR"].startswith("s3://"):
        raise ValueError("S3_TEMP_DIR must be a valid S3 URL starting with 's3://'")


def get_safe_decimal_type(decimal_type: DecimalType) -> str:
    """Convert MySQL decimal type to safe Redshift decimal type."""
    precision = min(decimal_type.precision, 38)  # Redshift max precision
    scale = min(decimal_type.scale, precision)  # Scale must be <= precision
    return f"DECIMAL({precision},{scale})"


def is_full_table_copy(bookmark_id: int) -> bool:
    """Determine if this is a full table copy based on bookmark value."""
    return bookmark_id == 0


# Initialize Glue context
def process_with_parallel(df, glueContext, connection_options, num_partitions=None):
    """
    Process dataframe using Spark's native partitioning for parallel processing.

    Args:
        df: Spark DataFrame to process
        glueContext: Glue context for creating DynamicFrames
        connection_options: Redshift connection options
        num_partitions: Number of partitions to use (defaults to 2x number of workers)
    """
    # Calculate optimal number of partitions if not specified
    if not num_partitions:
        # Get number of workers from Spark configuration
        num_workers = int(
            glueContext.spark_session.sparkContext.getConf().get(
                "spark.executor.instances", "2"
            )
        )
        num_partitions = num_workers * 2  # 2 partitions per worker

    logger.info(
        f"Repartitioning data into {num_partitions} partitions for parallel processing"
    )

    # Repartition data for parallel processing using id column
    df = df.repartition(num_partitions, "id")

    # Convert to dynamic frame with optimized partitioning
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "parallel_process")

    logger.info(f"Writing data with {num_partitions} partitions")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="redshift",
        connection_options=connection_options,
        transformation_ctx="parallel_redshift_write",
    )


# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get job arguments
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",  # Required for job initialization
        "REDSHIFT_CONNECTION",
        "REDSHIFT_SCHEMA",
        "S3_TEMP_DIR",
        "SOURCE_JDBC_CREDENTIAL",
        "SOURCE_JDBC_URL",
        "TABLES",
    ],
)

# Validate configuration
validate_config(args)

# Parameters
redshift_conn = args["REDSHIFT_CONNECTION"]
redshift_schema = args["REDSHIFT_SCHEMA"]
s3_temp_dir = args["S3_TEMP_DIR"]
source_jdbc_credential = args["SOURCE_JDBC_CREDENTIAL"]
source_jdbc_url = args["SOURCE_JDBC_URL"]

# 🔑 Fetch DB credentials
db_credentials = get_db_credentials(source_jdbc_credential)
username = db_credentials["USERNAME"]
password = db_credentials["PASSWORD"]

# Parse and validate table list and bookmarks
table_configs = {}
invalid_tables = []

for item in args["TABLES"].split(","):
    try:
        if ":" in item:
            table, bookmark = item.split(":")
            table = table.strip()
            safe_table = sanitize_identifier(table)
            table_configs[safe_table] = int(bookmark.strip())
        else:
            table = item.strip()
            safe_table = sanitize_identifier(table)
            table_configs[safe_table] = 0  # default bookmark 0
    except ValueError as e:
        invalid_tables.append((item, str(e)))

if invalid_tables:
    logger.error("Invalid table names found:")
    for table, error in invalid_tables:
        logger.error(f"- {table}: {error}")
    sys.exit(1)

if not table_configs:
    logger.error("No valid tables provided. Exiting job.")
    sys.exit(1)

logger.info(f"Tables to process with bookmarks: {table_configs}")

# Initialize job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Track job metrics
successful_tables = []
failed_tables = []

# Loop through tables
for table, bookmark_id in table_configs.items():
    try:
        logger.info(f"Processing table: {table} with bookmark ID: {bookmark_id}")

        # Prepare SQL query with bookmark
        query = f"SELECT * FROM {table} WHERE id > {bookmark_id}"

        # Set up connection options
        connection_options = {
            "url": source_jdbc_url,
            "user": username,
            "password": password,
            "dbtable": table,
            "sampleQuery": query,
        }

        # Validate table exists
        if not validate_table_exists(glueContext, table, connection_options):
            raise ValueError(f"Table {table} does not exist in source database")

        # Read from MySQL using query filter on id
        logger.info(f"Reading data with query: {query}")

        try:
            source_dynamic_frame = glueContext.create_dynamic_frame.from_options(
                connection_type="mysql",
                connection_options=connection_options,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to read from source table {table}: {str(e)}")
        # Convert to DataFrame
        df = source_dynamic_frame.toDF()

        # Record count logging
        initial_count = df.count()
        logger.info(f"Read {initial_count} records from {table}")

        # Handle DecimalType conversions
        for field in df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                logger.info(
                    f"Converting column {field.name} from DecimalType to Double"
                )
                df = df.withColumn(field.name, col(field.name).cast("double"))

        # Convert back to DynamicFrame
        cleaned_dynamic_frame = DynamicFrame.fromDF(
            df, glueContext, f"cleaned_df_{table}"
        )

        # Prepare CREATE TABLE statement based on schema
        schema = cleaned_dynamic_frame.schema()
        columns = []
        for field in schema.fields:
            col_name = field.name
            col_type = str(field.dataType).lower()

            # Map MySQL types to Redshift types with improved decimal handling
            try:
                if isinstance(field.dataType, DecimalType):
                    rs_type = get_safe_decimal_type(field.dataType)
                elif "string" in col_type:
                    rs_type = "VARCHAR(255)"
                elif "int" in col_type:
                    rs_type = "INTEGER"
                elif "bigint" in col_type:
                    rs_type = "BIGINT"
                elif "double" in col_type:
                    rs_type = "DOUBLE PRECISION"
                elif "timestamp" in col_type:
                    rs_type = "TIMESTAMP"
                elif "boolean" in col_type:
                    rs_type = "BOOLEAN"
                else:
                    logger.warning(
                        f"Unknown data type {col_type} for column {col_name}, defaulting to VARCHAR(255)"
                    )
                    rs_type = "VARCHAR(255)"

                columns.append(f'"{col_name}" {rs_type}')
            except Exception as e:
                logger.error(f"Error mapping data type for column {col_name}: {str(e)}")
                raise

        # Handle full table copy vs incremental copy
        if is_full_table_copy(bookmark_id):
            create_table_stmt = f"""
            DROP TABLE IF EXISTS {redshift_schema}.{table};
            CREATE TABLE {redshift_schema}.{table} (
                {', '.join(columns)}
            ) DISTSTYLE KEY DISTKEY(id) SORTKEY(id);
            """
            preactions = create_table_stmt
            logger.info(f"Full table copy - dropping and recreating {table}")
        else:
            create_table_stmt = f"""
            CREATE TABLE IF NOT EXISTS {redshift_schema}.{table} (
                {', '.join(columns)}
            ) DISTSTYLE KEY DISTKEY(id) SORTKEY(id);
            """
            delete_stmt = (
                f"DELETE FROM {redshift_schema}.{table} WHERE id > {bookmark_id};"
            )
            preactions = f"{create_table_stmt} {delete_stmt}"
            logger.info(f"Incremental copy - deleting records with id > {bookmark_id}")

        logger.info(f"Executing Redshift preactions for {table}")

        # Redshift connection options with optimized settings for performance
        connection_options = {
            "redshiftTmpDir": s3_temp_dir,
            "useConnectionProperties": "true",
            "dbtable": f"{redshift_schema}.{table}",
            "connectionName": redshift_conn,
            "preactions": preactions,
            "extracopyoptions": "TRUNCATECOLUMNS MAXERROR 1",
            "postactions": f"ANALYZE {redshift_schema}.{table};",
        }

        # Write to Redshift
        try:
            # Process based on table size
            if initial_count > 1000000:
                logger.info(f"Processing table {table} with parallel execution")
                process_with_parallel(
                    df,
                    glueContext,
                    connection_options,
                    num_partitions=4,  # Will auto-calculate based on workers
                )
            else:
                # Single write for smaller tables
                glueContext.write_dynamic_frame.from_options(
                    frame=cleaned_dynamic_frame,
                    connection_type="redshift",
                    connection_options=connection_options,
                    transformation_ctx=f"redshift_write_{table}",
                )
            successful_tables.append((table, initial_count))
            logger.info(f"✅ Successfully processed table: {table}")
        except Exception as e:
            raise RuntimeError(f"Failed to write to Redshift table {table}: {str(e)}")

    except Exception as e:
        failed_tables.append((table, str(e)))
        logger.error(f"❌ Error processing table {table}: {str(e)}")
        continue

# Log final job statistics
logger.info("=== Job Summary ===")
logger.info(f"Successfully processed {len(successful_tables)} tables:")
for table, count in successful_tables:
    logger.info(f"- {table}: {count} records")

if failed_tables:
    logger.error(f"Failed to process {len(failed_tables)} tables:")
    for table, error in failed_tables:
        logger.error(f"- {table}: {error}")
    job.commit()
    sys.exit(1)

logger.info("✅ All tables processed successfully")
job.commit()
