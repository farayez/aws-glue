import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node MySQL
MySQL_node1747495725157 = glueContext.create_dynamic_frame.from_catalog(
    database="gokada-delivery-catalogue",
    table_name="gokada_delivery_users",
    transformation_ctx="MySQL_node1747495725157",
)

# Script generated for node users
users_node1747472950803 = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "users",
        "connectionName": "gokada-prod-analytics-gokada-delivery-connection",
    },
    transformation_ctx="users_node1747472950803",
)

# Script generated for node Select Fields
SelectFields_node1747473653309 = SelectFields.apply(
    frame=users_node1747472950803,
    paths=[
        "id",
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "phone_verified",
        "email_verified",
        "account_type",
        "created_at",
        "country_code",
        "business_name",
        "is_business",
        "is_blocked",
        "deleted_at",
    ],
    transformation_ctx="SelectFields_node1747473653309",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1747495772575 = glueContext.write_dynamic_frame.from_options(
    frame=MySQL_node1747495725157,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-525408006129-eu-west-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "gokada_delivery_transformed.users_with_orders",
        "connectionName": "gokada-warehouse-connection",
        "preactions": "CREATE TABLE IF NOT EXISTS gokada_delivery_transformed.users_with_orders (tos_accepted_at VARCHAR, account_type VARCHAR, first_app_launched_at TIMESTAMP, api_token VARCHAR, android_token VARCHAR, created_at TIMESTAMP, device_type VARCHAR, enable_order_and_forget BOOLEAN, password VARCHAR, ios_token VARCHAR, updated_at TIMESTAMP, remember_token VARCHAR, first_name VARCHAR, email VARCHAR, is_business BOOLEAN, business_name VARCHAR, signup_region_id VARCHAR, email_verified BOOLEAN, device_id VARCHAR, api_token_expire_at VARCHAR, default_payment_method VARCHAR, first_app_installed_at TIMESTAMP, verified BOOLEAN, last_name VARCHAR, verification_type VARCHAR, deleted_at TIMESTAMP, version VARCHAR, country_code VARCHAR, is_blocked BOOLEAN, phone_verified BOOLEAN, tos_accepted BOOLEAN, device_token VARCHAR, name VARCHAR, phone_number VARCHAR, verification_code VARCHAR, phone_verify_token VARCHAR, id VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1747495772575",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1747472967856 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFields_node1747473653309,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-525408006129-eu-west-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "gokada_delivery_transformed.users_with_orders",
        "connectionName": "gokada-warehouse-connection",
        "preactions": "DROP TABLE IF EXISTS gokada_delivery_transformed.users_with_orders; CREATE TABLE IF NOT EXISTS gokada_delivery_transformed.users_with_orders (id DECIMAL, is_business BOOLEAN, first_name VARCHAR, last_name VARCHAR, business_name VARCHAR, email VARCHAR, email_verified BOOLEAN, country_code VARCHAR, phone_number VARCHAR, phone_verified BOOLEAN, account_type VARCHAR, is_blocked BOOLEAN, created_at TIMESTAMP, deleted_at TIMESTAMP);",
    },
    transformation_ctx="AmazonRedshift_node1747472967856",
)

job.commit()
