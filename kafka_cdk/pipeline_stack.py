"""
KafkaPipelineStack -- AWS CodePipeline CI for KafkaSimpleCdkStack.

  Source       GitHub (or any CodeConnections provider), on push to the branch
  UnitTests    CodeBuild: install requirements-dev.txt -> pytest. The tests
               synthesize offline from the committed cdk.context.json.

The pipeline never synthesizes for deploy, deploys, destroys, or changes
itself: deploy and destroy KafkaSimpleCdkStack (and change this pipeline) by
hand with the CDK CLI. See "CI with AWS CodePipeline" in the README.
"""
from aws_cdk import (
    Duration,
    Stack,
    aws_codebuild as codebuild,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
)
from constructs import Construct

PIPELINE_NAME = "kafka-simple-cdk"

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
