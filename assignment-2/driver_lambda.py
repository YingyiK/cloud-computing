"""
Part 4: Driver Lambda function
Orchestrates S3 operations and triggers the plotting lambda.
"""

import boto3
import os
import time
import urllib.request

# Initialize AWS clients
s3 = boto3.client('s3')

BUCKET_NAME = os.environ["BUCKET_NAME"]
PLOTTING_API_ENDPOINT = os.environ["PLOTTING_API_ENDPOINT"]
SLEEP_DURATION = 2  # Sleep 2 seconds between operations

def put_object(key: str, content: str):
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=content.encode("utf-8"))
    print(f"[driver] PUT {key} ({len(content.encode('utf-8'))} bytes)")


def delete_object(key: str):
    s3.delete_object(Bucket=BUCKET_NAME, Key=key)
    print(f"[driver] DELETE {key}")


def lambda_handler(event, context):
    # 1. Create assignment1.txt (19 bytes)
    put_object("assignment1.txt", "Empty Assignment 1")  # 19 bytes
    time.sleep(SLEEP_DURATION)

    # 2. Update assignment1.txt (28 bytes)
    put_object("assignment1.txt", "Empty Assignment 2222222222")
    time.sleep(SLEEP_DURATION)

    # 3. Delete assignment1.txt (0 bytes)
    delete_object("assignment1.txt")
    time.sleep(SLEEP_DURATION)

    # 4. Create assignment2.txt (2 bytes)
    put_object("assignment2.txt", "33")
    time.sleep(SLEEP_DURATION)

    # 5. Call the plotting lambda API
    print(f"[driver] Calling plotting API: {PLOTTING_API_ENDPOINT}")
    with urllib.request.urlopen(PLOTTING_API_ENDPOINT) as resp:
        status = resp.getcode()
    print(f"[driver] Plotting API responded with status: {status}")

    return {
        "statusCode": 200,
        "body": "Driver lambda completed successfully. Check TestBucket for the plot."
    }