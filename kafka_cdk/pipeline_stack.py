"""
KafkaPipelineStack -- AWS CodePipeline CI/CD for KafkaSimpleCdkStack.

Built with CDK Pipelines (aws_cdk.pipelines), so the pipeline is part of this
CDK app and updates itself: deploy it once from a laptop, and every later
change to it (including this file) ships through the pipeline.

  Source       GitHub (or any CodeConnections provider), on push to the branch
  Build        CodeBuild "Synth": unit tests -> cdk synth (context-drift
               check) -> cfn-lint. Its cdk.out is the only thing later
               stages deploy.
  UpdatePipeline
               self-mutation: the pipeline redeploys itself if it changed
  Prod         CloudFormation change set for KafkaSimpleCdkStack -> manual
               approval (review the change set) -> execute -> smoke test
               over SSM

Tear-down is a separate CodeBuild project (kafka-simple-cdk-destroy) that you
start by hand. See "CI/CD with AWS CodePipeline" in the README.
"""
from aws_cdk import (
    DefaultStackSynthesizer,
    Duration,
    Stack,
    Stage,
    aws_codebuild as codebuild,
    aws_iam as iam,
    pipelines,
)
from aws_cdk import aws_codepipeline as codepipeline
from constructs import Construct

from kafka_cdk.kafka_cluster_stack import SECRET_NAME, KafkaSimpleCdkStack

STACK_NAME = "KafkaSimpleCdkStack"
PIPELINE_NAME = "kafka-simple-cdk"
DESTROY_PROJECT_NAME = f"{PIPELINE_NAME}-destroy"
CDK_CLI_VERSION = "2.1145.0"
BOOTSTRAP_QUALIFIER = DefaultStackSynthesizer.DEFAULT_QUALIFIER
STACK_DESCRIPTION = (
    "Single-node, single-broker, KRaft-mode Apache Kafka on EC2 in the "
    "default VPC (learning example, not MSK). "
    "Connect via SSM: aws ssm start-session --target <instance-id>"
)

# Every CodeBuild action (synth, self-mutation, assets, smoke test) runs on
# the same image and runtimes. The image already has the AWS CLI v2 and jq.
BUILD_IMAGE = codebuild.LinuxBuildImage.AMAZON_LINUX_2023_5
RUNTIME_VERSIONS = {"nodejs": "22", "python": "3.12"}


class KafkaStage(Stage):
    """The deployable unit the pipeline promotes: just the Kafka stack."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # stack_name keeps the CloudFormation name identical to a local
        # `cdk deploy`, instead of the stage-prefixed "Prod-KafkaSimpleCdkStack".
        self.kafka_stack = KafkaSimpleCdkStack(
            self, STACK_NAME, stack_name=STACK_NAME, description=STACK_DESCRIPTION
        )


class KafkaPipelineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        repo: str,
        branch: str,
        connection_arn: str,
        require_approval: bool = True,
        run_smoke_tests: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        source = pipelines.CodePipelineSource.connection(
            repo, branch, connection_arn=connection_arn
        )

        # -------------------------------------------------------------
        # Build & Verify
        # -------------------------------------------------------------
        synth = pipelines.CodeBuildStep(
            "Synth",
            input=source,
            install_commands=[
                f"npm install -g --no-fund --no-audit aws-cdk@{CDK_CLI_VERSION}",
                "python -m pip install --quiet -r requirements-dev.txt",
            ],
            commands=[
                "mkdir -p reports",
                "python -m pytest -v --junitxml=reports/junit.xml",
                # The stack looks up the default VPC at synth time. CI should be
                # served entirely from the committed cdk.context.json; if synth
                # had to do a fresh lookup, the deploy would not be reproducible
                # from git, so fail and print the file to commit.
                "cp cdk.context.json reports/cdk.context.before.json",
                "cdk synth --quiet",
                "cmp -s cdk.context.json reports/cdk.context.before.json"
                " || { echo 'ERROR: cdk synth changed cdk.context.json (new context lookup).'"
                " 'Commit this cdk.context.json so deploys are reproducible:';"
                " cat cdk.context.json; exit 1; }",
                # Fail on errors; warnings are printed but do not break the build.
                "find cdk.out -name '*.template.json' -print0"
                " | xargs -0 cfn-lint --non-zero-exit-code error --",
            ],
            primary_output_directory="cdk.out",
            partial_build_spec=codebuild.BuildSpec.from_object(
                {
                    "reports": {
                        "unit-tests": {
                            "files": ["reports/junit.xml"],
                            "file-format": "JUNITXML",
                        }
                    }
                }
            ),
            # Only used if cdk.context.json is missing a lookup: lets synth
            # reach the default VPC through the bootstrap lookup role, so the
            # drift check above can print the value to commit.
            role_policy_statements=[
                iam.PolicyStatement(
                    actions=["sts:AssumeRole"],
                    resources=["*"],
                    conditions={
                        "StringEquals": {"iam:ResourceTag/aws-cdk:bootstrap-role": "lookup"}
                    },
                )
            ],
        )

        pipeline = pipelines.CodePipeline(
            self,
            "Pipeline",
            pipeline_name=PIPELINE_NAME,
            pipeline_type=codepipeline.PipelineType.V2,
            synth=synth,
            cli_version=CDK_CLI_VERSION,
            # Single account and region: no KMS key on the artifact bucket needed.
            cross_account_keys=False,
            code_build_defaults=pipelines.CodeBuildOptions(
                build_environment=codebuild.BuildEnvironment(build_image=BUILD_IMAGE),
                partial_build_spec=codebuild.BuildSpec.from_object(
                    {"phases": {"install": {"runtime-versions": RUNTIME_VERSIONS}}}
                ),
            ),
        )

        # -------------------------------------------------------------
        # Approve -> Deploy -> Smoke test
        # -------------------------------------------------------------
        stage = KafkaStage(self, "Prod", env=kwargs.get("env"))
        kafka_stack = stage.kafka_stack

        # Approval sits between "create change set" and "execute change set",
        # so the reviewer approves the exact CloudFormation change, viewable in
        # the console, rather than a diff of something else.
        change_set_steps = []
        if require_approval:
            change_set_steps.append(
                pipelines.ManualApprovalStep(
                    "Approve",
                    comment=(
                        f"Deploy {STACK_NAME}? Review its pending change set in the "
                        "CloudFormation console first."
                    ),
                )
            )

        post_steps = []
        if run_smoke_tests:
            post_steps.append(
                pipelines.CodeBuildStep(
                    "SmokeTest",
                    input=source,
                    env={"STACK_NAME": STACK_NAME},
                    env_from_cfn_outputs={"INSTANCE_ID": kafka_stack.instance_id_output},
                    commands=["bash scripts/smoke_test.sh"],
                    timeout=Duration.minutes(40),
                    role_policy_statements=self._smoke_test_policy(),
                )
            )

        pipeline.add_stage(
            stage,
            stack_steps=[pipelines.StackSteps(stack=kafka_stack, change_set=change_set_steps)],
            post=post_steps,
        )

        # -------------------------------------------------------------
        # Destroy (manual, outside the pipeline)
        # -------------------------------------------------------------
        self._destroy_project()

    def _smoke_test_policy(self) -> list:
        return [
            iam.PolicyStatement(
                actions=["ssm:SendCommand"],
                resources=[
                    f"arn:{self.partition}:ssm:{self.region}::document/AWS-RunShellScript"
                ],
            ),
            iam.PolicyStatement(
                actions=["ssm:SendCommand"],
                resources=[f"arn:{self.partition}:ec2:{self.region}:{self.account}:instance/*"],
                conditions={
                    "StringEquals": {"ssm:resourceTag/Project": "kafka-simple-no-msk"}
                },
            ),
            iam.PolicyStatement(
                actions=["ssm:GetCommandInvocation", "ssm:DescribeInstanceInformation"],
                resources=["*"],
            ),
        ]

    def _destroy_project(self) -> codebuild.Project:
        """
        `aws codebuild start-build --project-name kafka-simple-cdk-destroy
        --environment-variables-override name=CONFIRM_STACK_NAME,value=KafkaSimpleCdkStack`

        Deletes the stack with plain CloudFormation, so it needs no source
        checkout or synth. CloudFormation reuses the role the pipeline deployed
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
