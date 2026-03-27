## Midterm — S3 backup system (Replicator + Cleaner)

This CDK app deploys an object backup system:

- **BucketSrc**: source bucket (emits S3 events via EventBridge)
- **BucketDst**: destination bucket (stores backup copies)
- **DynamoDB Table**: records mapping from original object key → copy object key(s)
- **Replicator Lambda**: triggered by S3 PUT/DELETE events in BucketSrc
- **Cleaner Lambda**: runs every minute; deletes downed copies after 10 seconds

### DynamoDB design (no scan)

Single table with:

- **PK**: `src_key` (original object key)
- **SK**: `sk`
  - Copy rows: `COPY#<epoch_ms_13digits>#<uuid>`

GSI `ByDownedAt`:

- **GSI1PK**: `gsi1pk` (set to `DOWNED` only when a copy is downed)
- **GSI1SK**: `gsi1sk` (number, `downed_at_s`)

Cleaner queries the GSI:

- `gsi1pk = "DOWNED" AND gsi1sk < (now - 10)`

When a copy is deleted, Cleaner removes `gsi1pk/gsi1sk` so it will not be returned by future queries.

### Stacks

- `Midterm-Storage`: two S3 buckets + DynamoDB table (+ GSI)
- `Midterm-Replicator`: Replicator Lambda + EventBridge rule for BucketSrc events
- `Midterm-Cleaner`: Cleaner Lambda + 1-minute schedule

### Deploy

From your machine:

```bash
cd midterm
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cdk bootstrap
cdk deploy --all
```

### Basic test flow

1. Upload an object to **BucketSrc** (console or CLI).
2. Replicator copies it to **BucketDst** and writes a mapping row in DynamoDB.
3. Upload the same object 4 times: the oldest copy in **BucketDst** is deleted (keeps 3 newest).
4. Delete the object in **BucketSrc**: Replicator marks existing copies as `DOWNED` in DynamoDB.
5. Wait > 10 seconds: Cleaner deletes those copies from **BucketDst** and updates DynamoDB so they are not returned by the GSI query.

