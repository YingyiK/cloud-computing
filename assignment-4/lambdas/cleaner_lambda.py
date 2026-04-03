from __future__ import annotations

import os

import boto3


_s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]


def _largest_object_key(bucket: str) -> str | None:
    paginator = _s3.get_paginator("list_objects_v2")
    largest_key = None
    largest_size = -1

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key")
            size = obj.get("Size", 0)
            if key is None:
                continue
            if size > largest_size:
                largest_size = size
                largest_key = key

    return largest_key


def lambda_handler(event, context):
    key = _largest_object_key(BUCKET_NAME)
    if not key:
        return {"ok": True, "deleted": False, "reason": "bucket empty"}

    _s3.delete_object(Bucket=BUCKET_NAME, Key=key)
    return {"ok": True, "deleted": True, "key": key}

