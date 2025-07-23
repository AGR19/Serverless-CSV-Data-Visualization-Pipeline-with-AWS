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
source_table_name = "my_data_pipeline_csv_processed_data" # Updated based on your input
source_database_name = "csv_pipeline_db"

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database=source_database_name,
    table_name=source_table_name,
    transformation_ctx="datasource0_read_from_catalog"
)

print(f"Schema of datasource0 (from {source_database_name}.{source_table_name}):")
datasource0.printSchema()
print(f"Count of records in datasource0: {datasource0.count()}")

# --- 2. TRANSFORM: Apply mappings and transformations ---
# Mapping based on your provided schema:
# (source_column_name, source_type, target_column_name, target_type)

transform_mapping = [
    ("id", "bigint", "id", "long"),  # Keeping 'id' as long (bigint in Spark/Glue often maps to long)
    ("productname", "string", "product_name", "string"),
    ("category", "string", "category", "string"),
    ("salesamount", "double", "sales_amount", "double"), # Source is already double, just renaming
    ("transactiondate", "string", "transaction_date", "string")
]

transformed_frame = ApplyMapping.apply(
    frame=datasource0,
    mappings=transform_mapping,
    transformation_ctx="transformed_frame_apply_mapping"
)

print("Schema of transformed_frame (after ApplyMapping):")
transformed_frame.printSchema()
print(f"Count of records in transformed_frame: {transformed_frame.count()}")




# --- 3. LOAD: Write data to S3 in Parquet format and update Glue Data Catalog ---
output_s3_path = "s3://my-data-pipeline-csv-final-data/"
target_database_name = "csv_pipeline_db"
target_table_name = "final_visualization_data" # This table will be created/updated

# If using filtered_frame above, change frame=transformed_frame to frame=filtered_frame
datasink_final_output = glueContext.write_dynamic_frame.from_options(
    frame=transformed_frame, # Or use filtered_frame if you applied a filter
    connection_type="s3",
    connection_options={
        "path": output_s3_path,
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    format="parquet",
    additional_options={
         "catalogDatabase": target_database_name,
         "catalogTableName": target_table_name
    },
    transformation_ctx="datasink_final_output_s3_parquet"
)

job.commit()