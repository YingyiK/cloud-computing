"""
Size-tracking Lambda function (Assignment 4).
Consumes S3 event notifications from SQS (SNS fanout), computes bucket total size,
and writes (bucket_name, timestamp, total_size, object_count) to DynamoDB.
"""

from __future__ import annotations

import json
import os
import time

import boto3


s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")

TABLE_NAME = os.environ["TABLE_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
GLOBAL_MAX_BUCKET = "__GLOBAL_MAX__"


def _unwrap_s3_event_from_sns_sqs(record: dict) -> dict | None:
    """
    Expected: SQS record -> body (SNS envelope JSON) -> Message (stringified S3 event JSON).
    Returns parsed S3 event dict, or None.
    """
    body = record.get("body")
    if not body:
        return None
    try:
        sns_envelope = json.loads(body)
        msg = sns_envelope.get("Message")
        if not msg:
            return None
        return json.loads(msg)
    except Exception:
        return None


def compute_bucket_size(bucket_name: str) -> tuple[int, int]:
    """
    Compute the total size of all objects in the bucket.
    Returns: (total_size, object_count)
    """
    total_size = 0
    object_count = 0
    
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)

    for page in pages:
        for obj in page.get("Contents", []) or []:
            total_size += int(obj.get("Size", 0) or 0)
            object_count += 1

    return total_size, object_count


def write_to_dynamodb(bucket_name: str, total_size: int, object_count: int, timestamp: int):
    """
    Write bucket size information to DynamoDB table.
    """
    dynamodb.put_item(
        TableName=TABLE_NAME,
        Item={
            "bucket_name": {"S": bucket_name},
            "timestamp": {"N": str(timestamp)},
            "total_size": {"N": str(total_size)},
            "object_count": {"N": str(object_count)},
        },
    )
    print(
        f"✓ Written to DynamoDB: bucket={bucket_name}, size={total_size}, "
        f"count={object_count}, timestamp={timestamp}"
    )


def get_current_global_max() -> int:
    """
    Get the latest global max record using query only.
    Returns maximum total size seen so far.
    """
    response = dynamodb.query(
        TableName=TABLE_NAME,
        KeyConditionExpression="bucket_name = :bucket",
        ExpressionAttributeValues={":bucket": {"S": GLOBAL_MAX_BUCKET}},
        ScanIndexForward=False,
        Limit=1,
    )
    items = response.get("Items", [])
    if not items:
        return 0
    return int(items[0].get("total_size", {}).get("N", "0"))


def update_global_max_if_needed(total_size: int, timestamp: int):
    """
    Write a new global max record only when a higher max appears.
    """
    current_max = get_current_global_max()
    if total_size > current_max:
        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                "bucket_name": {"S": GLOBAL_MAX_BUCKET},
                "timestamp": {"N": str(timestamp)},
                "total_size": {"N": str(total_size)},
                "object_count": {"N": "0"},
            },
        )
        print(f"✓ Updated global max to {total_size} bytes at timestamp {timestamp}")

def lambda_handler(event, context):
    """
    Lambda handler function triggered by SQS messages.
    """
    processed = 0

    for sqs_record in event.get("Records", []):
        s3_event = _unwrap_s3_event_from_sns_sqs(sqs_record)
        if not s3_event:
            continue

        for record in s3_event.get("Records", []):
            event_name = record.get("eventName", "")
            bucket_name = record.get("s3", {}).get("bucket", {}).get("name", "")

            if bucket_name != BUCKET_NAME:
                continue

            if ("ObjectCreated" not in event_name) and ("ObjectRemoved" not in event_name):
                continue

            total_size, object_count = compute_bucket_size(bucket_name)
            timestamp = int(time.time())
            write_to_dynamodb(bucket_name, total_size, object_count, timestamp)
            update_global_max_if_needed(total_size, timestamp)
            processed += 1

    return {"statusCode": 200, "body": json.dumps({"processed": processed})}
