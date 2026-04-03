from __future__ import annotations

from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subs
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class Assignment4Stack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # All Lambda handler modules live under assignment-4/lambdas/*.py
        lambda_src_dir = str((Path(__file__).resolve().parents[1] / "lambdas").resolve())

        # A) Storage
        bucket = s3.Bucket(
            self,
            "TestBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
        )

        table = dynamodb.Table(
            self,
            "S3ObjectSizeHistoryTable",
            partition_key=dynamodb.Attribute(
                name="bucket_name", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.NUMBER),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # B) Fanout: S3 -> SNS -> (SQS size queue, SQS log queue)
        topic = sns.Topic(self, "S3EventFanoutTopic")

        size_dlq = sqs.Queue(self, "SizeTrackingDLQ", retention_period=Duration.days(14))
        log_dlq = sqs.Queue(self, "LoggingDLQ", retention_period=Duration.days(14))

        size_queue = sqs.Queue(
            self,
            "SizeTrackingQueue",
            visibility_timeout=Duration.seconds(90),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5, queue=size_dlq),
        )
        log_queue = sqs.Queue(
            self,
            "LoggingQueue",
            visibility_timeout=Duration.seconds(90),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5, queue=log_dlq),
        )

        topic.add_subscription(sns_subs.SqsSubscription(size_queue))
        topic.add_subscription(sns_subs.SqsSubscription(log_queue))

        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SnsDestination(topic),
        )
        bucket.add_event_notification(
            s3.EventType.OBJECT_REMOVED,
            s3n.SnsDestination(topic),
        )

        # C) Consumers
        size_tracking_fn = _lambda.Function(
            self,
            "SizeTrackingLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="size_tracking_lambda.lambda_handler",
            code=_lambda.Code.from_asset(lambda_src_dir),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "TABLE_NAME": table.table_name,
            },
        )
        size_tracking_fn.add_event_source(
            lambda_event_sources.SqsEventSource(size_queue, batch_size=10)
        )
        bucket.grant_read(size_tracking_fn)
        table.grant_read_write_data(size_tracking_fn)

        logging_fn = _lambda.Function(
            self,
            "LoggingLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="logging_lambda.lambda_handler",
            code=_lambda.Code.from_asset(lambda_src_dir),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "BUCKET_NAME": bucket.bucket_name,
            },
        )
        logging_fn.add_event_source(
            lambda_event_sources.SqsEventSource(log_queue, batch_size=10)
        )
        bucket.grant_read(logging_fn)
        logging_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:FilterLogEvents"],
                resources=["*"],
            )
        )

        # D) Metric filter + metric + alarm action (Alarm -> Cleaner lambda)
        logs.MetricFilter(
            self,
            "TotalObjectSizeMetricFilter",
            log_group=logging_fn.log_group,
            metric_namespace="Assignment4App",
            metric_name="TotalObjectSize",
            filter_pattern=logs.FilterPattern.exists("$.size_delta"),
            metric_value="$.size_delta",
        )

        # Base metric: deltas extracted from LoggingLambda logs.
        total_object_size_delta_metric = cloudwatch.Metric(
            namespace="Assignment4App",
            metric_name="TotalObjectSize",
            statistic="Sum",
            period=Duration.minutes(1),
        )

        # Metric math: running total size (cumulative sum of deltas).
        #
        # NOTE: CloudWatch docs caution about RUNNING_SUM in alarms due to extra data retrieval
        # during evaluations. For this assignment's small test workload, it is a practical way
        # to approximate "current total object size" from delta samples.
        total_object_size_running = cloudwatch.MathExpression(
            expression="RUNNING_SUM(m1)",
            using_metrics={"m1": total_object_size_delta_metric},
            period=Duration.minutes(1),
        )

        cleaner_fn = _lambda.Function(
            self,
            "CleanerLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="cleaner_lambda.lambda_handler",
            code=_lambda.Code.from_asset(lambda_src_dir),
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={"BUCKET_NAME": bucket.bucket_name},
        )
        bucket.grant_read_write(cleaner_fn)

        alarm = cloudwatch.Alarm(
            self,
            "TotalObjectSizeAbove20",
            metric=total_object_size_running,
            threshold=20,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        alarm.add_alarm_action(cw_actions.LambdaAction(cleaner_fn))

        # E) Plotting API (API Gateway -> plotting lambda)
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
                "PLOT_KEY": "plot",
                "PLOT_SECONDS": "300",
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

        plot_api_url = f"{api.url}plot"

        # F) Driver
        driver_fn = _lambda.Function(
            self,
            "DriverLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="driver_lambda.lambda_handler",
            code=_lambda.Code.from_asset(lambda_src_dir),
            timeout=Duration.seconds(420),
            memory_size=256,
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "PLOTTING_API_ENDPOINT": plot_api_url,
                "SLEEP_DURATION": "5",
                "WAIT_TIMEOUT_SECONDS": "420",
                "ALARM_PERIOD_SECONDS": "60",
                "ALIGN_TO_PERIOD": "true",
                "MIN_ALIGN_SLEEP_SECONDS": "20",
                "ALIGN_OFFSET_SECONDS": "5",
            },
        )
        bucket.grant_read_write(driver_fn)

        # Outputs
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "TableName", value=table.table_name)
        CfnOutput(self, "FanoutTopicArn", value=topic.topic_arn)
        CfnOutput(self, "SizeQueueName", value=size_queue.queue_name)
        CfnOutput(self, "LogQueueName", value=log_queue.queue_name)
        CfnOutput(self, "PlotApiUrl", value=plot_api_url)
        CfnOutput(self, "DriverLambdaName", value=driver_fn.function_name)
