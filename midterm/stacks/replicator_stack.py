from __future__ import annotations

from pathlib import Path

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from constructs import Construct


class ReplicatorStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        src_bucket: s3.IBucket,
        dst_bucket: s3.IBucket,
        table: dynamodb.ITable,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_src_dir = str((Path(__file__).resolve().parents[1] / "lambdas").resolve())

        replicator_fn = _lambda.Function(
            self,
            "ReplicatorLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="replicator.lambda_handler",
            code=_lambda.Code.from_asset(lambda_src_dir),
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "TABLE_NAME": table.table_name,
                "SRC_BUCKET": src_bucket.bucket_name,
                "DST_BUCKET": dst_bucket.bucket_name,
                "MAX_COPIES": "3",
                "DOWNED_GSI_PK": "DOWNED",
            },
        )

        table.grant_read_write_data(replicator_fn)
        src_bucket.grant_read(replicator_fn)
        dst_bucket.grant_read_write(replicator_fn)

        rule = events.Rule(
            self,
            "BucketSrcEventsToReplicator",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created", "Object Deleted"],
                detail={"bucket": {"name": [src_bucket.bucket_name]}},
            ),
        )
        rule.add_target(targets.LambdaFunction(replicator_fn))

        CfnOutput(self, "ReplicatorLambdaName", value=replicator_fn.function_name)

