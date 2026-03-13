"""
Part 1: Setup script to create S3 bucket and DynamoDB table
This is a Python program (not a Lambda) that runs on your laptop.
"""

import boto3
import json
from botocore.exceptions import ClientError

# Configuration
BUCKET_NAME = 'assignment2-bucket-kyy-20260219'
TABLE_NAME = 'S3-object-size-history'
REGION = 'us-west-2'

def create_s3_bucket():
    """Create the S3 bucket if it doesn't exist."""
    s3 = boto3.client('s3', region_name=REGION)
    
    try:
        # Check if bucket exists
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"✓ Bucket '{BUCKET_NAME}' already exists")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket doesn't exist, create it
            try:
                if REGION == 'us-east-1':
                    # us-east-1 doesn't need LocationConstraint
                    s3.create_bucket(Bucket=BUCKET_NAME)
                else:
                    s3.create_bucket(
                        Bucket=BUCKET_NAME,
                        CreateBucketConfiguration={'LocationConstraint': REGION}
                    )
                print(f"✓ Bucket '{BUCKET_NAME}' created successfully")
            except ClientError as create_error:
                print(f"✗ Error creating bucket: {create_error}")
                raise
        else:
            print(f"✗ Error checking bucket: {e}")
            raise

def create_dynamodb_table():
    """Create the DynamoDB table for storing bucket size history."""
    dynamodb = boto3.client('dynamodb', region_name=REGION)
    
    # Table schema design:
    # - bucket_name (Partition Key): String - allows tracking multiple buckets
    # - timestamp (Sort Key): Number - allows querying by time range
    # - total_size: Number - total size of all objects in bytes
    # - object_count: Number - number of objects in the bucket
    # 
    # This design allows:
    # - Querying size history for a specific bucket ordered by timestamp
    # - Querying size history for a specific bucket within a time range
    # - Supporting multiple buckets in the same table
    
    table_definition = {
        'TableName': TABLE_NAME,
        'KeySchema': [
            {
                'AttributeName': 'bucket_name',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'bucket_name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'N'
            }
        ],
        'BillingMode': 'PAY_PER_REQUEST'  # On-demand pricing
    }
    
    try:
        # Check if table exists
        dynamodb.describe_table(TableName=TABLE_NAME)
        print(f"✓ Table '{TABLE_NAME}' already exists")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # Table doesn't exist, create it
            try:
                response = dynamodb.create_table(**table_definition)
                print(f"✓ Creating table '{TABLE_NAME}'...")
                
                # Wait for table to be active
                waiter = dynamodb.get_waiter('table_exists')
                waiter.wait(TableName=TABLE_NAME)
                print(f"✓ Table '{TABLE_NAME}' created and active")
            except ClientError as create_error:
                print(f"✗ Error creating table: {create_error}")
                raise
        else:
            print(f"✗ Error checking table: {e}")
            raise

def main():
    """Main function to create all resources."""
    print("\n=== Setting up S3 Bucket and DynamoDB Table ===\n")
    
    try:
        create_s3_bucket()
        create_dynamodb_table()
        print("\n✓ All resources created successfully!")
        print(f"  - S3 Bucket: {BUCKET_NAME}")
        print(f"  - DynamoDB Table: {TABLE_NAME}")
    except Exception as e:
        print(f"\n✗ Setup failed: {e}")
        raise

if __name__ == "__main__":
    main()
