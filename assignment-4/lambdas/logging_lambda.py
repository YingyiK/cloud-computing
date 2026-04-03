from __future__ import annotations

import json
import os
from urllib.parse import unquote_plus

import boto3


_logs = boto3.client("logs")

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")


def _unwrap_s3_event_from_sns_sqs(record: dict) -> dict | None:
    """
    Expected shape: SQS record -> body (SNS envelope JSON) -> Message (stringified S3 event JSON).
    Returns parsed S3 event dict or None.
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


def _creation_size_from_logs(log_group_name: str, object_name: str) -> int | None:
    """
    Best-effort lookup for the most recent creation log that contains the object_name.
    This is used for delete events since S3 delete notifications don't include object size.
    """
    try:
        resp = _logs.filter_log_events(
            logGroupName=log_group_name,
            filterPattern=f'"object_name" "{object_name}"',
            limit=50,
        )
        events = resp.get("events") or []
        for ev in reversed(events):
            msg = ev.get("message") or ""
            try:
                payload = json.loads(msg)
            except Exception:
                continue
            if payload.get("object_name") != object_name:
                continue
            delta = payload.get("size_delta")
            if isinstance(delta, int) and delta > 0:
                return delta
        return None
    except Exception:
        return None


def _log_delta(object_name: str, size_delta: int):
    # IMPORTANT: keep it pure JSON for MetricFilter to extract $.size_delta
    print(json.dumps({"object_name": object_name, "size_delta": size_delta}))


def lambda_handler(event, context):
    # Consume from SQS; each record is SNS-wrapped S3 event JSON
    for sqs_record in event.get("Records", []):
        s3_event = _unwrap_s3_event_from_sns_sqs(sqs_record)
        if not s3_event:
            continue

        for rec in s3_event.get("Records", []):
            event_name = rec.get("eventName", "")
            s3_info = rec.get("s3", {}) or {}
            bucket = (s3_info.get("bucket", {}) or {}).get("name", "")
            obj = s3_info.get("object", {}) or {}
            key = obj.get("key") or ""
            object_name = unquote_plus(key)

            if BUCKET_NAME and bucket and bucket != BUCKET_NAME:
                continue

            if "ObjectCreated" in event_name:
                size = obj.get("size")
                if isinstance(size, int):
                    _log_delta(object_name, size)
                continue

            if "ObjectRemoved" in event_name:
                size = _creation_size_from_logs(context.log_group_name, object_name)
                if isinstance(size, int):
                    _log_delta(object_name, -size)
                else:
                    # Fallback: still log something JSON so you can see it happened.
                    _log_delta(object_name, 0)

    return {"statusCode": 200}

