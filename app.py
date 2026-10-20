#!/usr/bin/env python3
"""
CDK app entry point for the kafka-simple-no-msk learning example, with an
AWS CodePipeline that runs the unit tests.

Two top-level stacks, both deployed by hand:

  KafkaSimpleCdkStack   the Kafka broker: `cdk deploy KafkaSimpleCdkStack`
  KafkaPipelineStack    the CI pipeline:  `cdk deploy KafkaPipelineStack`

KafkaPipelineStack is only added once "pipeline.connection_arn" in cdk.json is
set to an AWS CodeConnections ARN (see "CI with AWS CodePipeline" in the
README), so the Kafka stack can be deployed without any pipeline setup.

Region is fixed to us-east-1 per the spec. Account is taken from the CDK
CLI / environment (CDK_DEFAULT_ACCOUNT). The Kafka stack performs a LIVE
lookup of the account's default VPC at synth time, cached in the committed
cdk.context.json.
"""
import os

import aws_cdk as cdk

from kafka_cdk.kafka_cluster_stack import KafkaSimpleCdkStack
from kafka_cdk.pipeline_stack import KafkaPipelineStack

app = cdk.App()

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region="us-east-1",
)

KafkaSimpleCdkStack(
    app,
    "KafkaSimpleCdkStack",
    env=env,
    description=(
        "Single-node, single-broker, KRaft-mode Apache Kafka on EC2 in the "
        "default VPC (learning example, not MSK). "
        "Connect via SSM: aws ssm start-session --target <instance-id>"
    ),
)

settings = app.node.try_get_context("pipeline") or {}
connection_arn = settings.get("connection_arn", "")
if connection_arn.startswith("arn:"):
    KafkaPipelineStack(
        app,
        "KafkaPipelineStack",
        repo=settings["repo"],
        branch=settings.get("branch", "main"),
        connection_arn=connection_arn,
        env=env,
        description="CodePipeline that runs the unit tests for KafkaSimpleCdkStack",
    )

app.synth()
