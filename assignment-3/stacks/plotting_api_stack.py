from __future__ import annotations

from pathlib import Path

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_dynamodb as dynamodb
from constructs import Construct


class PlottingApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket: s3.IBucket,
        table: dynamodb.ITable,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_src_dir = str(
            (Path(__file__).resolve().parents[2] / "assignment-2").resolve()
        )

        plotting_fn = _lambda.Function(
            self,
            "PlottingLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="plotting_lambda.lambda_handler",
            code=_lambda.Code.from_asset(lambda_src_dir),
            timeout=Duration.seconds(30),
            memory_size=512,
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "TABLE_NAME": table.table_name,
                "PLOT_KEY": "plot",  # key only (not a physical resource name)
            },
        )

        table.grant_read_data(plotting_fn)
        bucket.grant_put(plotting_fn)

        api = apigw.RestApi(
            self,
            "RestApi",
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=["GET", "OPTIONS"],
            ),
            deploy_options=apigw.StageOptions(stage_name="prod"),
        )

        plot = api.root.add_resource("plot")
        plot.add_method("GET", apigw.LambdaIntegration(plotting_fn))

        self.plot_api_url = f"{api.url}plot"

        CfnOutput(self, "PlotApiUrl", value=self.plot_api_url)

