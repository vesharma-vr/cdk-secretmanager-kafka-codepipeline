"""
Unit tests for KafkaPipelineStack -- the CodePipeline that runs this app's
unit tests. Like the stack tests, these synthesize offline from the committed
cdk.context.json.
"""

import json
import pathlib

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Template

from kafka_cdk.pipeline_stack import PIPELINE_NAME, KafkaPipelineStack

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
REGION = "us-east-1"
CONNECTION_ARN = (
    "arn:aws:codeconnections:us-east-1:111122223333:connection/"
    "00000000-0000-0000-0000-000000000000"
)


@pytest.fixture(scope="module")
def template():
    context = json.loads((PROJECT_ROOT / "cdk.context.json").read_text())
    vpc_key = next(k for k in context if k.startswith("vpc-provider:"))
    account = vpc_key.split("account=", 1)[1].split(":", 1)[0]
    app = cdk.App(context=context)
    stack = KafkaPipelineStack(
        app,
        "KafkaPipelineStack",
        repo="example-org/example-repo",
        branch="main",
        connection_arn=CONNECTION_ARN,
        env=cdk.Environment(account=account, region=REGION),
    )
    return Template.from_stack(stack)


def _stages(template):
    pipeline = next(iter(template.find_resources("AWS::CodePipeline::Pipeline").values()))
    return {s["Name"]: s["Actions"] for s in pipeline["Properties"]["Stages"]}


def test_pipeline_is_source_then_unit_tests_only(template):
    template.has_resource_properties(
        "AWS::CodePipeline::Pipeline",
        {"Name": PIPELINE_NAME, "PipelineType": "V2"},
    )
    stages = _stages(template)
    assert list(stages) == ["Source", "UnitTests"]

    source = stages["Source"][0]
    assert source["ActionTypeId"]["Provider"] == "CodeStarSourceConnection"
    assert source["Configuration"]["ConnectionArn"] == CONNECTION_ARN
    assert source["Configuration"]["FullRepositoryId"] == "example-org/example-repo"
    assert source["Configuration"]["BranchName"] == "main"

    assert [a["ActionTypeId"]["Provider"] for a in stages["UnitTests"]] == ["CodeBuild"]


def test_unit_tests_project_runs_pytest_and_nothing_else(template):
    projects = template.find_resources("AWS::CodeBuild::Project")
    specs = [
        p["Properties"]["Source"]["BuildSpec"]
        for p in projects.values()
        if "pytest" in p["Properties"]["Source"].get("BuildSpec", "")
    ]
    assert len(specs) == 1
    spec = specs[0]
    assert "python -m pytest" in spec
    for absent in ("cdk synth", "cdk deploy", "cfn-lint"):
        assert absent not in spec

