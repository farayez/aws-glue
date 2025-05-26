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
    for table_spec in spec.strip().split(';'):
        parts = table_spec.split(':')
        table_config = {
            "name": parts[0],
            "columns": ['*'],  # default to all columns
            "last_id": 0      # default to 0
        }
        # If columns are specified
        if len(parts) >= 2 and parts[1]:  # Check if not empty
            table_config["columns"] = parts[1].split(',') if parts[1] != '*' else ['*']
        # If last_id is specified
        if len(parts) >= 3 and parts[2]:  # Check if not empty
            table_config["last_id"] = int(parts[2])
        
        tables.append(table_config)
    return tables


def log_output(message: str):
    print(f"[INFO] {message}")


class GlueRDSToRedshift:
    def __init__(self):
        self.parse_arguments()

        # Initialize Glue context and job
        self.init_context(self.job_name)

    def parse_arguments(self):
        params = ['JOB_NAME', 'RUNTIME_ENV', 'SOURCE_DB', 'SOURCE_TABLE_PREFIX', 'TABLES']
        args = getResolvedOptions(sys.argv, params)
        self.environment = 'PRODUCTION' if 'RUNTIME_ENV' not in args else args['RUNTIME_ENV'].upper()

        # Parse table specifications
        self.tables = parse_table_spec(args['TABLES'])
        self.source_db = args['SOURCE_DB']
        self.source_table_prefix = args['SOURCE_TABLE_PREFIX']
        log_output(f"Parsed tables: {self.tables}")
        log_output(f"Source DB: {self.source_db}, Source Table Prefix: {self.source_table_prefix}")

        self.job_name = args['JOB_NAME']
        

    def init_context(self, job_name):
        if self.environment != 'PROD':
            return

        self.context = GlueContext(SparkContext.getOrCreate())
        self.job = Job(self.context)
        self.job.init(job_name)

    def commit_job(self):
        if self.environment != 'PROD':
            return
        self.job.commit()
        
    def read_from_rds(self, table_config):
        if self.environment != 'PROD':
            return

        # Use Glue Catalog connection
        return self.context.create_dynamic_frame.from_catalog(
            database=self.source_db,
            table_name=self.source_table_prefix + table_config["name"],
            push_down_predicate=f"id > {table_config['last_id']}",
            select_fields=None if table_config["columns"] == ['*'] else table_config["columns"]
        )
        
    def write_to_redshift(self, dynamic_frame, table_name):
        if self.environment != 'PROD':
            return
        # return
        
        # Use Glue Catalog connection
        self.context.write_dynamic_frame.from_catalog(
            frame=dynamic_frame,
            database="your_redshift_db",
            table_name=table_name
        )
        
    def run(self):
        try:
            for table_config in self.tables:
                log_output(f"Reading from RDS: {table_config['name']} with last_id {table_config['last_id']} and columns {table_config['columns']}")
                df = self.read_from_rds(table_config)
                df.printSchema() if df else log_output("No data returned from RDS")
                self.write_to_redshift(df, table_config["name"])
            self.commit_job()
        except Exception as e:
            self.commit_job()
            raise e


if __name__ == '__main__':
    GlueRDSToRedshift().run()