"""
KafkaPipelineStack -- AWS CodePipeline CI for KafkaSimpleCdkStack.

  Source       GitHub (or any CodeConnections provider), on push to the branch
  UnitTests    CodeBuild: install requirements-dev.txt -> pytest. The tests
               synthesize offline from the committed cdk.context.json.

The pipeline never synthesizes for deploy, deploys, or changes itself: deploy
KafkaSimpleCdkStack (and changes to this pipeline) by hand with `cdk deploy`.

Tear-down is a separate CodeBuild project (kafka-simple-cdk-destroy) that you
start by hand. See "CI with AWS CodePipeline" in the README.
"""
from aws_cdk import (
    DefaultStackSynthesizer,
    Duration,
    Stack,
    aws_codebuild as codebuild,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_iam as iam,
)
from constructs import Construct

from kafka_cdk.kafka_cluster_stack import SECRET_NAME

STACK_NAME = "KafkaSimpleCdkStack"
PIPELINE_NAME = "kafka-simple-cdk"
DESTROY_PROJECT_NAME = f"{PIPELINE_NAME}-destroy"
BOOTSTRAP_QUALIFIER = DefaultStackSynthesizer.DEFAULT_QUALIFIER

# Every CodeBuild project runs on the same image. The image already has the
# AWS CLI v2, which the destroy project uses.
BUILD_IMAGE = codebuild.LinuxBuildImage.AMAZON_LINUX_2023_5
RUNTIME_VERSIONS = {"nodejs": "22", "python": "3.12"}


class KafkaPipelineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        repo: str,
        branch: str,
        connection_arn: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        owner, repo_name = repo.split("/", 1)
        source_output = codepipeline.Artifact("Source")

        # Node is only there for the jsii runtime the Python CDK library uses.
        unit_tests = codebuild.PipelineProject(
            self,
            "UnitTestsProject",
            project_name=f"{PIPELINE_NAME}-unit-tests",
            description="Runs the CDK app's unit tests (pytest).",
            environment=codebuild.BuildEnvironment(build_image=BUILD_IMAGE),
            timeout=Duration.minutes(30),
            build_spec=codebuild.BuildSpec.from_object(
                {
                    "version": "0.2",
                    "phases": {
                        "install": {
                            "runtime-versions": RUNTIME_VERSIONS,
                            "commands": [
                                "python -m pip install --quiet -r requirements-dev.txt",
                            ],
                        },
                        "build": {
                            "commands": [
                                "mkdir -p reports",
                                "python -m pytest -v --junitxml=reports/junit.xml",
                            ]
                        },
                    },
                    "reports": {
                        "unit-tests": {
                            "files": ["reports/junit.xml"],
                            "file-format": "JUNITXML",
                        }
                    },
                }
            ),
        )

        codepipeline.Pipeline(
            self,
            "Pipeline",
            pipeline_name=PIPELINE_NAME,
            pipeline_type=codepipeline.PipelineType.V2,
            # Single account and region: no KMS key on the artifact bucket needed.
            cross_account_keys=False,
            stages=[
                codepipeline.StageProps(
                    stage_name="Source",
                    actions=[
                        codepipeline_actions.CodeStarConnectionsSourceAction(
                            action_name="Source",
                            owner=owner,
                            repo=repo_name,
                            branch=branch,
                            connection_arn=connection_arn,
                            output=source_output,
                        )
                    ],
                ),
                codepipeline.StageProps(
                    stage_name="UnitTests",
                    actions=[
                        codepipeline_actions.CodeBuildAction(
                            action_name="UnitTests",
                            project=unit_tests,
                            input=source_output,
                        )
                    ],
                ),
            ],
        )

        # -------------------------------------------------------------
        # Destroy (manual, outside the pipeline)
        # -------------------------------------------------------------
        self._destroy_project()

    def _destroy_project(self) -> codebuild.Project:
        """
        `aws codebuild start-build --project-name kafka-simple-cdk-destroy
        --environment-variables-override name=CONFIRM_STACK_NAME,value=KafkaSimpleCdkStack`

        Deletes the stack with plain CloudFormation, so it needs no source
        checkout or synth. CloudFormation reuses the role `cdk deploy` deployed
        the stack with (the bootstrap cfn-exec role), which is why this project
        needs iam:PassRole on that role and nothing EC2/IAM-specific.
        """
        project = codebuild.Project(
            self,
            "DestroyProject",
            project_name=DESTROY_PROJECT_NAME,
            description=(
                f"Tears down {STACK_NAME}. Start a build with CONFIRM_STACK_NAME="
                f"{STACK_NAME}; set FORCE_DELETE_SECRET=true to also purge the secret."
            ),
            environment=codebuild.BuildEnvironment(build_image=BUILD_IMAGE),
            timeout=Duration.minutes(75),
            environment_variables={
                "STACK_NAME": codebuild.BuildEnvironmentVariable(value=STACK_NAME),
                "SECRET_NAME": codebuild.BuildEnvironmentVariable(value=SECRET_NAME),
                "CONFIRM_STACK_NAME": codebuild.BuildEnvironmentVariable(value=""),
                "FORCE_DELETE_SECRET": codebuild.BuildEnvironmentVariable(value="false"),
            },
            build_spec=codebuild.BuildSpec.from_object(
                {
                    "version": "0.2",
                    "phases": {
                        "build": {
                            "commands": [
                                '[ "$CONFIRM_STACK_NAME" = "$STACK_NAME" ]'
                                " || { echo \"CONFIRM_STACK_NAME='$CONFIRM_STACK_NAME' does not"
                                " match $STACK_NAME; nothing destroyed.\"; exit 1; }",
                                'aws cloudformation delete-stack --stack-name "$STACK_NAME"',
                                'aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME"',
                                'status=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME"'
                                " --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo GONE)",
                                'echo "Stack status after destroy: $status"',
                                'case "$status" in GONE|DELETE_COMPLETE) ;; *) exit 1 ;; esac',
                                # See README "Secrets Manager gotcha": the fixed secret
                                # name stays reserved for 30 days unless purged.
                                'if [ "$FORCE_DELETE_SECRET" = "true" ]; then'
                                ' aws secretsmanager delete-secret --secret-id "$SECRET_NAME"'
                                " --force-delete-without-recovery"
                                ' || echo "Secret $SECRET_NAME not found -- nothing to purge.";'
                                " fi",
                            ]
                        }
                    },
                }
            ),
        )

        cfn_exec_role = (
            f"arn:{self.partition}:iam::{self.account}:role/"
            f"cdk-{BOOTSTRAP_QUALIFIER}-cfn-exec-role-{self.account}-{self.region}"
        )
        project.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudformation:DeleteStack", "cloudformation:DescribeStacks"],
                resources=[
                    f"arn:{self.partition}:cloudformation:{self.region}:{self.account}"
                    f":stack/{STACK_NAME}/*"
                ],
            )
        )
        project.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[cfn_exec_role],
                conditions={
                    "StringEquals": {"iam:PassedToService": "cloudformation.amazonaws.com"}
                },
            )
        )
        project.add_to_role_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:DeleteSecret"],
                resources=[
                    f"arn:{self.partition}:secretsmanager:{self.region}:{self.account}"
                    f":secret:{SECRET_NAME}-*"
                ],
            )
        )
        return project
