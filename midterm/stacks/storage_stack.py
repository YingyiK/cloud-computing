from __future__ import annotations

from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_s3 as s3
from constructs import Construct


class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.src_bucket = s3.Bucket(
            self,
            "BucketSrc",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            event_bridge_enabled=True,
        )

        self.dst_bucket = s3.Bucket(
            self,
            "BucketDst",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
        )

        self.table = dynamodb.Table(
            self,
            "BackupMappingTable",
            partition_key=dynamodb.Attribute(
                name="src_key", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.table.add_global_secondary_index(
            index_name="ByDownedAt",
            partition_key=dynamodb.Attribute(
                name="gsi1pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="gsi1sk", type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        CfnOutput(self, "BucketSrcName", value=self.src_bucket.bucket_name)
        CfnOutput(self, "BucketDstName", value=self.dst_bucket.bucket_name)
        CfnOutput(self, "TableName", value=self.table.table_name)

