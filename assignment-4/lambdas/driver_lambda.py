"""
Part 4: Driver Lambda function
Orchestrates S3 operations and triggers the plotting lambda.
"""

import boto3
import os
import time
import urllib.request
from botocore.exceptions import ClientError

# Initialize AWS clients
s3 = boto3.client('s3')

BUCKET_NAME = os.environ["BUCKET_NAME"]
PLOTTING_API_ENDPOINT = os.environ["PLOTTING_API_ENDPOINT"]
SLEEP_DURATION = int(os.environ.get("SLEEP_DURATION", "5"))
WAIT_TIMEOUT_SECONDS = int(os.environ.get("WAIT_TIMEOUT_SECONDS", "120"))
ALARM_PERIOD_SECONDS = int(os.environ.get("ALARM_PERIOD_SECONDS", "60"))
ALIGN_TO_PERIOD = os.environ.get("ALIGN_TO_PERIOD", "true").lower() == "true"
MIN_ALIGN_SLEEP_SECONDS = int(os.environ.get("MIN_ALIGN_SLEEP_SECONDS", "20"))
ALIGN_OFFSET_SECONDS = int(os.environ.get("ALIGN_OFFSET_SECONDS", "5"))

def put_object(key: str, content: str):
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=content.encode("utf-8"))
    print(f"[driver] PUT {key} ({len(content.encode('utf-8'))} bytes)")


def _object_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except ClientError as e:
        code = (e.response.get("Error") or {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def wait_until_deleted(key: str):
    deadline = time.time() + WAIT_TIMEOUT_SECONDS
    while time.time() < deadline:
        if not _object_exists(key):
            print(f"[driver] CONFIRMED deleted: {key}")
            return
        time.sleep(2)
    print(f"[driver] TIMEOUT waiting for deletion: {key}")


def _sleep_to_next_period_boundary():
    """
    CloudWatch Logs metric filters emit metrics on 1-minute periods.
    To reliably trigger the alarm twice, keep the -delta and +delta in different periods.
    """
    if not ALIGN_TO_PERIOD:
        return

    now = time.time()
    period = max(ALARM_PERIOD_SECONDS, 60)
    next_boundary = (int(now // period) + 1) * period
    sleep_s = next_boundary - now

    # If we're too close to the boundary, wait an extra full period so the
    # deletion (-delta) has time to land in the current period.
    if sleep_s < MIN_ALIGN_SLEEP_SECONDS:
        next_boundary += period
        sleep_s = next_boundary - now

    total_sleep = sleep_s + ALIGN_OFFSET_SECONDS
    print(f"[driver] Aligning to next period: sleeping {total_sleep:.1f}s")
    time.sleep(total_sleep)


def lambda_handler(event, context):
    # 1) Create assignment1.txt. Content required by assignment.
    put_object("assignment1.txt", "Empty Assignment 1")
    time.sleep(SLEEP_DURATION)

    # 2) Create assignment2.txt. Content required by assignment.
    put_object("assignment2.txt", "Empty Assignment 2222222222")
    time.sleep(SLEEP_DURATION)

    # 3) Alarm should fire; Cleaner should delete assignment2.txt
    wait_until_deleted("assignment2.txt")

    # 4) Create assignment3.txt (2 bytes)
    _sleep_to_next_period_boundary()
    put_object("assignment3.txt", "33")
    time.sleep(SLEEP_DURATION)

    # 5) Alarm should fire; Cleaner should delete assignment1.txt
    wait_until_deleted("assignment1.txt")

    # 6) Call the plotting lambda API
    print(f"[driver] Calling plotting API: {PLOTTING_API_ENDPOINT}")
    with urllib.request.urlopen(PLOTTING_API_ENDPOINT) as resp:
        status = resp.getcode()
    print(f"[driver] Plotting API responded with status: {status}")

    return {
        "statusCode": 200,
        "body": "Driver lambda completed successfully. Check TestBucket for the plot.",
    }