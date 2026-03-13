"""
Part 2: Size-tracking Lambda function
Triggered by S3 events (object creation, update, deletion) in assignment2-bucket-kyy-20260219.
Computes total size of all objects and writes to DynamoDB.
"""

import boto3
import json
import os
import time
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

TABLE_NAME = os.environ["TABLE_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
GLOBAL_MAX_BUCKET = '__GLOBAL_MAX__'

def compute_bucket_size(bucket_name):
    """
    Compute the total size of all objects in the bucket.
    Returns: (total_size, object_count)
    """
    total_size = 0
    object_count = 0
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj['Size']
                    object_count += 1
        
        return total_size, object_count
    except Exception as e:
        print(f"Error computing bucket size: {e}")
        raise

def write_to_dynamodb(bucket_name, total_size, object_count, timestamp):
    """
    Write bucket size information to DynamoDB table.
    """
    try:
        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'bucket_name': {'S': bucket_name},
                'timestamp': {'N': str(timestamp)},
                'total_size': {'N': str(total_size)},
                'object_count': {'N': str(object_count)}
            }
        )
        print(f"✓ Written to DynamoDB: bucket={bucket_name}, size={total_size}, count={object_count}, timestamp={timestamp}")
    except Exception as e:
        print(f"Error writing to DynamoDB: {e}")
        raise

def get_current_global_max():
    """
    Get the latest global max record using query only.
    Returns maximum total size seen so far.
    """
    try:
        response = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression='bucket_name = :bucket',
            ExpressionAttributeValues={
                ':bucket': {'S': GLOBAL_MAX_BUCKET}
            },
            ScanIndexForward=False,  # latest timestamp first
            Limit=1
        )
        items = response.get('Items', [])
        if not items:
            return 0
        return int(items[0].get('total_size', {}).get('N', '0'))
    except Exception as e:
        print(f"Error querying global max: {e}")
        raise

def update_global_max_if_needed(total_size, timestamp):
    """
    Write a new global max record only when a higher max appears.
    """
    try:
        current_max = get_current_global_max()
        if total_size > current_max:
            dynamodb.put_item(
                TableName=TABLE_NAME,
                Item={
                    'bucket_name': {'S': GLOBAL_MAX_BUCKET},
                    'timestamp': {'N': str(timestamp)},
                    'total_size': {'N': str(total_size)},
                    'object_count': {'N': '0'}
                }
            )
            print(f"✓ Updated global max to {total_size} bytes at timestamp {timestamp}")
    except Exception as e:
        print(f"Error updating global max: {e}")
        raise

def lambda_handler(event, context):
    """
    Lambda handler function triggered by S3 events.
    """
    print(f"Received S3 event: {json.dumps(event)}")
    
    # Process each S3 event record
    for record in event.get('Records', []):
        event_name = record.get('eventName', '')
        bucket_name = record.get('s3', {}).get('bucket', {}).get('name', '')
        
        # Only process events for our target bucket
        if bucket_name != BUCKET_NAME:
            print(f"Skipping event for bucket: {bucket_name}")
            continue
        
        # Check if this is a relevant event (object created, updated, or deleted)
        if any(event_type in event_name for event_type in ['ObjectCreated', 'ObjectRemoved']):
            print(f"Processing event: {event_name} for bucket: {bucket_name}")
            
            # Compute total size of all objects in the bucket
            total_size, object_count = compute_bucket_size(bucket_name)
            
            # Get current timestamp (Unix epoch time in seconds)
            timestamp = int(time.time())
            
            # Write to DynamoDB
            write_to_dynamodb(bucket_name, total_size, object_count, timestamp)
            update_global_max_if_needed(total_size, timestamp)
            
            print(f"✓ Size tracking completed: {total_size} bytes, {object_count} objects")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Size tracking completed successfully')
    }
