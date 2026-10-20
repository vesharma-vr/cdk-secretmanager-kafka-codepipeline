"""
Unit tests for KafkaPipelineStack -- the CodePipeline that delivers
KafkaSimpleCdkStack. Like the stack tests, these synthesize offline from the
committed cdk.context.json.
"""

import json
import pathlib

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from kafka_cdk.pipeline_stack import (
    DESTROY_PROJECT_NAME,
    PIPELINE_NAME,
    STACK_NAME,
    KafkaPipelineStack,
)

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
REGION = "us-east-1"
CONNECTION_ARN = (
    "arn:aws:codeconnections:us-east-1:111122223333:connection/"
    "00000000-0000-0000-0000-000000000000"
)


def _synth(**overrides):
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
        **overrides,
    )
    return app, stack, Template.from_stack(stack)


@pytest.fixture(scope="module")
def synthed():
    return _synth()


def _stages(template):
    pipeline = next(iter(template.find_resources("AWS::CodePipeline::Pipeline").values()))
    return {s["Name"]: s["Actions"] for s in pipeline["Properties"]["Stages"]}


def test_pipeline_shape(synthed):
    _, _, template = synthed
    template.has_resource_properties(
        "AWS::CodePipeline::Pipeline",
        {"Name": PIPELINE_NAME, "PipelineType": "V2"},
    )
    assert list(_stages(template)) == ["Source", "Build", "UpdatePipeline", "Prod"]

    source = _stages(template)["Source"][0]
    assert source["ActionTypeId"]["Provider"] == "CodeStarSourceConnection"
    assert source["Configuration"]["ConnectionArn"] == CONNECTION_ARN
    assert source["Configuration"]["FullRepositoryId"] == "example-org/example-repo"
    assert source["Configuration"]["BranchName"] == "main"


def test_prod_stage_approves_the_change_set_then_smoke_tests(synthed):
    _, _, template = synthed
    actions = sorted(_stages(template)["Prod"], key=lambda a: a["RunOrder"])
    steps = [
        (a["Name"], a["ActionTypeId"]["Provider"], a.get("Configuration", {}).get("ActionMode"))
        for a in actions
    ]
    assert steps == [
        (f"{STACK_NAME}.Prepare", "CloudFormation", "CHANGE_SET_REPLACE"),
        (f"{STACK_NAME}.Approve", "Manual", None),
        (f"{STACK_NAME}.Deploy", "CloudFormation", "CHANGE_SET_EXECUTE"),
        ("SmokeTest", "CodeBuild", None),
    ]
    # Same CloudFormation name as a local deploy, not "Prod-KafkaSimpleCdkStack".
    assert actions[0]["Configuration"]["StackName"] == STACK_NAME


def test_approval_and_smoke_test_can_be_switched_off():
    _, _, template = _synth(require_approval=False, run_smoke_tests=False)
    providers = [
        (a["ActionTypeId"]["Provider"], a["Configuration"].get("ActionMode"))
        for a in sorted(_stages(template)["Prod"], key=lambda a: a["RunOrder"])
    ]
    assert providers == [
        ("CloudFormation", "CHANGE_SET_REPLACE"),
        ("CloudFormation", "CHANGE_SET_EXECUTE"),
    ]


def test_synth_runs_tests_drift_check_and_lint(synthed):
    _, _, template = synthed
    projects = template.find_resources("AWS::CodeBuild::Project")
    synth_specs = [
        p["Properties"]["Source"]["BuildSpec"]
        for p in projects.values()
        if "cdk synth" in p["Properties"]["Source"].get("BuildSpec", "")
    ]
    assert len(synth_specs) == 1
    spec = synth_specs[0]
    for expected in ("python -m pytest", "cmp -s cdk.context.json", "cfn-lint --non-zero-exit-code error"):
        assert expected in spec


def test_smoke_test_gets_instance_id_and_scoped_ssm(synthed):
    _, _, template = synthed
    projects = template.find_resources("AWS::CodeBuild::Project")
    smoke = [p for p in projects.values() if "smoke_test.sh" in p["Properties"]["Source"].get("BuildSpec", "")]
    assert len(smoke) == 1

    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Action": "ssm:SendCommand",
                                "Condition": {
                                    "StringEquals": {"ssm:resourceTag/Project": "kafka-simple-no-msk"}
                                },
                            }
                        )
                    ]
                )
            }
        },
    )


def test_destroy_project_is_confirmed_and_narrowly_scoped(synthed):
    _, _, template = synthed
    template.has_resource_properties(
        "AWS::CodeBuild::Project",
        {
            "Name": DESTROY_PROJECT_NAME,
            "Source": {"BuildSpec": Match.string_like_regexp(r"CONFIRM_STACK_NAME")},
        },
    )
    statements = [
        s
        for p in template.find_resources("AWS::IAM::Policy").values()
        for s in p["Properties"]["PolicyDocument"]["Statement"]
        if s["Action"] in (["cloudformation:DeleteStack", "cloudformation:DescribeStacks"], "iam:PassRole")
    ]
    assert len(statements) == 2
    delete, pass_role = statements
    assert f":stack/{STACK_NAME}/*" in json.dumps(delete["Resource"])
    assert "cfn-exec-role" in json.dumps(pass_role["Resource"])
    assert pass_role["Condition"] == {
        "StringEquals": {"iam:PassedToService": "cloudformation.amazonaws.com"}
    }


def test_prod_stage_contains_the_kafka_stack(synthed):
    app, _, _ = synthed
    assembly = app.synth()
    nested = [a for a in assembly.nested_assemblies]
    assert len(nested) == 1
    stacks = nested[0].nested_assembly.stacks
    assert [s.stack_name for s in stacks] == [STACK_NAME]
    Template.from_json(stacks[0].template).resource_count_is("AWS::EC2::Instance", 1)
