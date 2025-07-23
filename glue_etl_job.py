import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 1. EXTRACT: Read data from the Glue Data Catalog ---
# NOTE: Ensure this table name matches the one created by your first crawler
source_table_name = "my_data_pipeline_csv_processed_data"
source_database_name = "csv_pipeline_db"

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database=source_database_name,
    table_name=source_table_name,
    transformation_ctx="datasource0_read_from_catalog"
)

# --- 2. TRANSFORM: Apply mappings and transformations ---
transform_mapping = [
    ("id", "bigint", "id", "long"),
    ("productname", "string", "product_name", "string"),
    ("category", "string", "category", "string"),
    ("salesamount", "double", "sales_amount", "double"),
    ("transactiondate", "string", "transaction_date", "string")
]

transformed_frame = ApplyMapping.apply(
    frame=datasource0,
    mappings=transform_mapping,
    transformation_ctx="transformed_frame_apply_mapping"
)

# --- 3. LOAD: Write data to S3 in Parquet format and update Glue Data Catalog ---
output_s3_path = "s3://my-data-pipeline-csv-final-data/"
target_database_name = "csv_pipeline_db"
target_table_name = "final_visualization_data" # This is the table the job should create

datasink_final_output = glueContext.write_dynamic_frame.from_options(
    frame=transformed_frame,
    connection_type="s3",
    connection_options={
        "path": output_s3_path,
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
        "catalog.database": target_database_name,
        "catalog.table": target_table_name
    },
    format="parquet",
    transformation_ctx="datasink_final_output_s3_parquet"
)

job.commit() 