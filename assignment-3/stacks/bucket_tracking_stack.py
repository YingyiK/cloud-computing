from __future__ import annotations

from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from constructs import Construct


class BucketTrackingStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        table: dynamodb.ITable,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Bucket + notifications must live with the target Lambda to avoid cross-stack cycles.
        self.bucket = s3.Bucket(
            self,
            "TestBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
        )

        lambda_src_dir = str(
            (Path(__file__).resolve().parents[2] / "assignment-2").resolve()
        )

        self.size_tracking_fn = _lambda.Function(
            self,
            "SizeTrackingLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="size_tracking_lambda.lambda_handler",
            code=_lambda.Code.from_asset(lambda_src_dir),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "BUCKET_NAME": self.bucket.bucket_name,
                "TABLE_NAME": table.table_name,
            },
        )

        # size_tracking_lambda uses both PutItem (write) and Query (read) for GLOBAL_MAX_BUCKET.
        table.grant_read_write_data(self.size_tracking_fn)
        self.bucket.grant_read(self.size_tracking_fn)

        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.size_tracking_fn),
        )
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_REMOVED,
            s3n.LambdaDestination(self.size_tracking_fn),
        )

        CfnOutput(self, "BucketName", value=self.bucket.bucket_name)

