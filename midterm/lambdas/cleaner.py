from __future__ import annotations

import os
import time

import boto3
from boto3.dynamodb.conditions import Key


_s3 = boto3.client("s3")
_dynamodb = boto3.resource("dynamodb")


def _table():
    return _dynamodb.Table(os.environ["TABLE_NAME"])


DST_BUCKET = os.environ["DST_BUCKET"]
DOWNED_GSI_PK = os.environ.get("DOWNED_GSI_PK", "DOWNED")
DOWNED_GSI_NAME = os.environ.get("DOWNED_GSI_NAME", "ByDownedAt")
DOWNED_AGE_SECONDS = int(os.environ.get("DOWNED_AGE_SECONDS", "10"))


def _query_expired(downed_before_s: int) -> list[dict]:
    items: list[dict] = []
    last_evaluated_key = None
    table = _table()

    while True:
        kwargs = {
            "IndexName": DOWNED_GSI_NAME,
            "KeyConditionExpression": Key("gsi1pk").eq(DOWNED_GSI_PK)
            & Key("gsi1sk").lt(downed_before_s),
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.query(**kwargs)
        items.extend(resp.get("Items") or [])
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return items


def lambda_handler(event, context):
    now_s = int(time.time())
    cutoff_s = now_s - DOWNED_AGE_SECONDS

    table = _table()
    expired = _query_expired(cutoff_s)

    deleted = 0
    for item in expired:
        src_key = item.get("src_key")
        sk = item.get("sk")
        dst_key = item.get("dst_key")
        if not (src_key and sk and dst_key):
            continue

        try:
            _s3.delete_object(Bucket=DST_BUCKET, Key=dst_key)
        except Exception:
            pass

        table.update_item(
            Key={"src_key": src_key, "sk": sk},
            UpdateExpression="SET #status=:deleted, deleted_at_s=:t REMOVE gsi1pk, gsi1sk",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":deleted": "DELETED", ":t": now_s},
        )
        deleted += 1

    return {"ok": True, "cutoff_s": cutoff_s, "candidates": len(expired), "deleted": deleted}

