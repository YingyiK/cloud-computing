from __future__ import annotations

from pathlib import Path

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from constructs import Construct


class DriverStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket: s3.IBucket,
        plot_api_url: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_src_dir = str(
            (Path(__file__).resolve().parents[2] / "assignment-2").resolve()
        )

        driver_fn = _lambda.Function(
            self,
            "DriverLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="driver_lambda.lambda_handler",
            code=_lambda.Code.from_asset(lambda_src_dir),
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "PLOTTING_API_ENDPOINT": plot_api_url,
            },
        )

        bucket.grant_read_write(driver_fn)

        CfnOutput(self, "DriverLambdaName", value=driver_fn.function_name)

