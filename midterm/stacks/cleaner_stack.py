from __future__ import annotations

from pathlib import Path

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from constructs import Construct


class CleanerStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        dst_bucket: s3.IBucket,
        table: dynamodb.ITable,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_src_dir = str((Path(__file__).resolve().parents[1] / "lambdas").resolve())

        cleaner_fn = _lambda.Function(
            self,
            "CleanerLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="cleaner.lambda_handler",
            code=_lambda.Code.from_asset(lambda_src_dir),
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "TABLE_NAME": table.table_name,
                "DST_BUCKET": dst_bucket.bucket_name,
                "DOWNED_GSI_PK": "DOWNED",
                "DOWNED_GSI_NAME": "ByDownedAt",
                "DOWNED_AGE_SECONDS": "10",
            },
        )

        table.grant_read_write_data(cleaner_fn)
        dst_bucket.grant_delete(cleaner_fn)

        schedule = events.Rule(
            self,
            "CleanerSchedule",
            schedule=events.Schedule.rate(Duration.minutes(1)),
        )
        schedule.add_target(targets.LambdaFunction(cleaner_fn))

        CfnOutput(self, "CleanerLambdaName", value=cleaner_fn.function_name)

