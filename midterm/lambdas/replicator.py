from __future__ import annotations

import os
import time
import uuid
from urllib.parse import unquote_plus

import boto3
from boto3.dynamodb.conditions import Key


_s3 = boto3.client("s3")
_dynamodb = boto3.resource("dynamodb")


def _table():
    return _dynamodb.Table(os.environ["TABLE_NAME"])


SRC_BUCKET = os.environ["SRC_BUCKET"]
DST_BUCKET = os.environ["DST_BUCKET"]
MAX_COPIES = int(os.environ.get("MAX_COPIES", "3"))
DOWNED_GSI_PK = os.environ.get("DOWNED_GSI_PK", "DOWNED")


def _event_src_key(event: dict) -> str | None:
    detail = event.get("detail") or {}
    obj = detail.get("object") or {}
    key = obj.get("key")
    if not key:
        return None
    return unquote_plus(key)


def _query_all_copies(src_key: str) -> list[dict]:
    items: list[dict] = []
    last_evaluated_key = None
    table = _table()

    while True:
        kwargs = {
            "KeyConditionExpression": Key("src_key").eq(src_key)
            & Key("sk").begins_with("COPY#"),
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.query(**kwargs)
        items.extend(resp.get("Items") or [])
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return items


def _handle_put(event: dict) -> dict:
    src_key = _event_src_key(event)
    if not src_key:
        return {"ok": False, "reason": "missing object key"}

    now_ms = int(time.time() * 1000)
    rand = uuid.uuid4().hex
    dst_key = f"{src_key}~{now_ms}~{rand}"
    sk = f"COPY#{now_ms:013d}#{rand}"

    _s3.copy_object(
        Bucket=DST_BUCKET,
        Key=dst_key,
        CopySource={"Bucket": SRC_BUCKET, "Key": src_key},
    )

    table = _table()
    table.put_item(
        Item={
            "src_key": src_key,
            "sk": sk,
            "dst_key": dst_key,
            "status": "ACTIVE",
            "created_at_ms": now_ms,
        }
    )

    copies = _query_all_copies(src_key)
    active = [c for c in copies if c.get("status") == "ACTIVE"]
    active.sort(key=lambda x: x.get("sk", ""))

    if len(active) > MAX_COPIES:
        to_remove = active[: len(active) - MAX_COPIES]
        for item in to_remove:
            old_dst_key = item.get("dst_key")
            old_sk = item.get("sk")
            if old_dst_key:
                try:
                    _s3.delete_object(Bucket=DST_BUCKET, Key=old_dst_key)
                except Exception:
                    pass
            if old_sk:
                table.delete_item(Key={"src_key": src_key, "sk": old_sk})

    return {"ok": True, "src_key": src_key, "dst_key": dst_key}


def _handle_delete(event: dict) -> dict:
    src_key = _event_src_key(event)
    if not src_key:
        return {"ok": False, "reason": "missing object key"}

    now_s = int(time.time())
    table = _table()

    copies = _query_all_copies(src_key)
    active = [c for c in copies if c.get("status") == "ACTIVE"]

    updated = 0
    for item in active:
        sk = item.get("sk")
        if not sk:
            continue

        table.update_item(
            Key={"src_key": src_key, "sk": sk},
            UpdateExpression=(
                "SET #status=:downed, downed_at_s=:t, gsi1pk=:pk, gsi1sk=:t"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":downed": "DOWNED",
                ":t": now_s,
                ":pk": DOWNED_GSI_PK,
            },
        )
        updated += 1

    return {"ok": True, "src_key": src_key, "downed": updated}


def lambda_handler(event, context):
    detail_type = event.get("detail-type")

    if detail_type == "Object Created":
        return _handle_put(event)
    if detail_type == "Object Deleted":
        return _handle_delete(event)

    return {"ok": True, "ignored": True, "detail_type": detail_type}

