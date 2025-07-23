import boto3
import csv
import io

# Initialize the S3 client outside the handler for better performance
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # 1. Get the bucket name and object key (filename) from the S3 event
    source_bucket_name = event['Records'][0]['s3']['bucket']['name']
    source_object_key = event['Records'][0]['s3']['object']['key']

    # 2. Define the target bucket for the processed data
    processed_bucket_name = 'my-data-pipeline-csv-processed-data'
    processed_object_key = f"processed-{source_object_key}"

    print(f"Received event for object: s3://{source_bucket_name}/{source_object_key}")
    print(f"Will write processed data to: s3://{processed_bucket_name}/{processed_object_key}")

    try:
        # 3. Read the CSV file from the source S3 bucket
        response = s3_client.get_object(Bucket=source_bucket_name, Key=source_object_key)
        csv_content = response['Body'].read().decode('utf-8')
        print(f"Successfully read {len(csv_content)} bytes from source.")

        # 4. Perform basic processing on the CSV data (simple passthrough in this example)
        input_file = io.StringIO(csv_content)
        output_file = io.StringIO()

        reader = csv.reader(input_file)
        writer = csv.writer(output_file)

        # Process all rows
        for row in reader:
            writer.writerow(row)

        processed_csv_content = output_file.getvalue()

        # 5. Write the processed data to the target S3 bucket
        if processed_csv_content:
            s3_client.put_object(
                Bucket=processed_bucket_name,
                Key=processed_object_key,
                Body=processed_csv_content.encode('utf-8')
            )
            print(f"Successfully processed '{source_object_key}' and uploaded to '{processed_bucket_name}/{processed_object_key}'")
            return {
                'statusCode': 200,
                'body': f"Successfully processed {source_object_key}"
            }
        else:
            print(f"No content to write for '{source_object_key}' after processing. Skipping S3 put.")
            return {
                'statusCode': 200,
                'body': f"No content to write for {source_object_key} after processing."
            }

    except Exception as e:
        print(f"Error processing object '{source_object_key}' from bucket '{source_bucket_name}': {e}")
        raise e