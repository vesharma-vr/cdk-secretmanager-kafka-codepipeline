#!/usr/bin/env python3
"""
CDK app entry point for the kafka-simple-no-msk learning example, delivered
through AWS CodePipeline.

The app's only top-level stack is KafkaPipelineStack. KafkaSimpleCdkStack
lives inside the pipeline's "Prod" stage, so a plain `cdk deploy` deploys the
pipeline, and the pipeline deploys Kafka. To deploy the Kafka stack directly
from a laptop instead, name it: `cdk deploy KafkaPipelineStack/Prod/KafkaSimpleCdkStack`.

Pipeline settings (repo, branch, CodeConnections ARN, approval / smoke-test
switches) come from the "pipeline" context in cdk.json. They must be
committed, because the pipeline re-synthesizes this app from the repo.

Region is fixed to us-east-1 per the spec. Account is taken from the CDK
CLI / environment (CDK_DEFAULT_ACCOUNT). The Kafka stack performs a LIVE
lookup of the account's default VPC at synth time, cached in the committed
cdk.context.json.
"""
import os

import aws_cdk as cdk

from kafka_cdk.pipeline_stack import KafkaPipelineStack

app = cdk.App()

settings = app.node.try_get_context("pipeline") or {}
connection_arn = settings.get("connection_arn", "")
if not connection_arn.startswith("arn:"):
    raise ValueError(
        'Set "pipeline.connection_arn" in cdk.json to your AWS CodeConnections '
        "connection ARN (see 'CI/CD with AWS CodePipeline' in the README)."
    )

KafkaPipelineStack(
    app,
    "KafkaPipelineStack",
    repo=settings["repo"],
    branch=settings.get("branch", "main"),
    connection_arn=connection_arn,
    require_approval=settings.get("require_approval", True),
    run_smoke_tests=settings.get("run_smoke_tests", True),
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region="us-east-1",
    ),
    description="CodePipeline (CDK Pipelines) that builds, tests and deploys KafkaSimpleCdkStack",
)

app.synth()
