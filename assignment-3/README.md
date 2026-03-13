## Assignment 3 (CDK) — Assignment 2 resources

This CDK app deploys the same resources as Assignment 2, without manual console setup:

- 3 Lambda functions (`driver`, `size_tracking`, `plotting`)
- 1 S3 bucket (logical id `TestBucket`) + event trigger to `size_tracking`
- 1 DynamoDB table + a secondary index
- 1 REST API (API Gateway) exposing `GET /plot` backed by the plotting Lambda

### Project layout

- `app.py`: CDK entrypoint
- `stacks/`: CDK stacks (split into multiple stacks)
- Lambda source code is reused from `../assignment-2/`

### Stacks

- `Assignment3-Data`: DynamoDB table + GSI
- `Assignment3-BucketTracking`: S3 bucket + S3→Lambda notifications + size-tracking Lambda
- `Assignment3-PlottingApi`: plotting Lambda + REST API (`GET /plot`)
- `Assignment3-Driver`: driver Lambda

### How to deploy (from your machine)

Install prerequisites:

- Python 3.10+
- Node.js 18+
- AWS CDK CLI v2 (`npm i -g aws-cdk`)

Create a virtualenv and install CDK libs:

```bash
cd assignment-3
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Bootstrap (once per account/region):

```bash
cdk bootstrap
```

Deploy:

```bash
cdk deploy --all
```

After deployment:

- The stack outputs include the **S3 bucket name**, **DynamoDB table name**, and the **Plot API URL**.
- Invoke `driver` Lambda manually to generate events and then call `GET /plot` (or call `GET /plot` directly after activity).

