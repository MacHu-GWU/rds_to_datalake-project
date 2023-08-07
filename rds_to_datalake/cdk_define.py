# -*- coding: utf-8 -*-

# ------------------------------------------------------------------------------
# Import dependencies
# ------------------------------------------------------------------------------
import json
import dataclasses

import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_iam as iam
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_rds as rds
import aws_cdk.aws_dms as dms
import aws_cdk.aws_glue as glue

from constructs import Construct

from .config_define import Config
from .config_init import config
from .boto_ses import bsm
from .iam import is_role_exists
from .s3_bucket import is_bucket_exists
from . import paths
from . import s3paths


class Stack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        config: Config,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.config = config

    def declare_s3_bucket(self):
        if is_bucket_exists(bsm.s3_client, config.s3_bucket_artifacts) is False:
            self.s3_bucket_artifacts = s3.Bucket(
                self,
                f"S3BucketArtifacts",
                bucket_name=config.s3_bucket_artifacts,
            )
        else:
            self.s3_bucket_artifacts = s3.Bucket.from_bucket_name(
                self,
                f"S3BucketArtifacts",
                bucket_name=config.s3_bucket_artifacts,
            )

        if is_bucket_exists(bsm.s3_client, config.s3_bucket_data) is False:
            self.s3_bucket_data = s3.Bucket(
                self,
                f"S3BucketData",
                bucket_name=config.s3_bucket_data,
            )
        else:
            self.s3_bucket_data = s3.Bucket.from_bucket_name(
                self,
                f"S3BucketData",
                bucket_name=config.s3_bucket_data,
            )

        for bucket, description in [
            (config.s3_bucket_glue_assets, "GlueAssets"),
        ]:
            if is_bucket_exists(bsm.s3_client, bucket) is False:
                bucket = s3.Bucket(
                    self,
                    f"S3Bucket{description}",
                    bucket_name=bucket,
                )

    def declare_iam_role(self):
        if is_role_exists(bsm.iam_client, config.dms_vpc_role_name) is False:
            self.dms_vpc_role = iam.Role(
                self,
                "DMSVpcRole",
                role_name=config.dms_vpc_role_name,
                assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AdministratorAccess"
                    ),
                ],
            )

        if (
            is_role_exists(bsm.iam_client, config.dms_cloudwatch_logs_role_name)
            is False
        ):
            self.dms_cloudwatch_logs_role = iam.Role(
                self,
                "DMSVpcCloudWatchLogsRole",
                role_name=config.dms_cloudwatch_logs_role_name,
                assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AdministratorAccess"
                    ),
                ],
            )

        self.dms_s3_endpoint_role = iam.Role(
            self,
            "DMSS3EndpointRole",
            role_name=config.dms_s3_endpoint_role_name,
            assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
            ],
        )

        self.lambda_role = iam.Role(
            self,
            "LambdaRole",
            role_name=self.config.lambda_role_name,
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
            ],
        )

        self.glue_role = iam.Role(
            self,
            f"GlueRole",
            role_name=self.config.glue_role_name,
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
            ],
        )

    def declare_rds_database(self):
        self.db_subnet_group = rds.CfnDBSubnetGroup(
            self,
            "DBSubnetGroup",
            db_subnet_group_name=self.config.db_subnet_group_name,
            db_subnet_group_description="on public subnet",
            subnet_ids=self.config.subnet_id_list,
        )
        self.db_parameter_group = rds.CfnDBParameterGroup(
            self,
            "DBParameterGroup",
            db_parameter_group_name=self.config.db_parameter_group_name,
            description="enable cdc data stream",
            family="postgres15",
            parameters={
                "rds.logical_replication": "1",
            },
        )
        self.db_security_group = ec2.CfnSecurityGroup(
            self,
            "DBSecurityGroup",
            group_name=self.config.db_security_group_name,
            group_description="security group for rds",
            security_group_ingress=[
                ec2.CfnSecurityGroup.IngressProperty(
                    ip_protocol="-1",
                    from_port=-1,
                    to_port=-1,
                    cidr_ip=f"{authorized_ip}/32",
                )
                for authorized_ip in self.config.authorized_ip_list
            ],
        )

        self.db_security_group_default_ingress = ec2.CfnSecurityGroupIngress(
            self,
            "DBSecurityGroupDefaultIngress",
            ip_protocol="-1",
            from_port=-1,
            to_port=-1,
            group_id=self.db_security_group.attr_group_id,
            source_security_group_id=self.db_security_group.attr_group_id,
        )
        self.db_security_group_default_ingress.add_depends_on(self.db_security_group)

        self.db_instance = rds.CfnDBInstance(
            self,
            "DBInstance",
            engine="postgres",
            engine_version="15.3",
            allocated_storage="20",
            db_instance_identifier=self.config.db_instance_identifier,
            db_instance_class="db.t3.small",
            db_parameter_group_name=self.db_parameter_group.db_parameter_group_name,
            db_subnet_group_name=self.db_subnet_group.db_subnet_group_name,
            vpc_security_groups=[
                self.db_security_group.attr_group_id,
            ],
            master_username=self.config.username,
            master_user_password=self.config.password,
            multi_az=False,
            publicly_accessible=True,
            deletion_protection=False,
        )
        self.db_instance.add_depends_on(self.db_subnet_group)
        self.db_instance.add_depends_on(self.db_parameter_group)
        self.db_instance.add_depends_on(self.db_security_group)
        self.db_instance.add_depends_on(self.db_security_group_default_ingress)

        self.output_db_instance_host = cdk.CfnOutput(
            self,
            config.db_instance_host_output_id,
            value=self.db_instance.attr_endpoint_address,
        )

    def declare_dms(self):
        # ref: https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_dms/CfnReplicationSubnetGroup.html
        self.dms_subnet_group = dms.CfnReplicationSubnetGroup(
            self,
            "DMSSubnetGroup",
            replication_subnet_group_identifier=self.config.dms_subnet_group_name,
            replication_subnet_group_description="on public subnet",
            subnet_ids=self.config.subnet_id_list,
        )

        # ref: https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_dms/CfnReplicationInstance.html
        self.dms_replication_instance = dms.CfnReplicationInstance(
            self,
            "ReplicationInstance",
            replication_instance_class="dms.t3.small",
            replication_subnet_group_identifier=self.dms_subnet_group.replication_subnet_group_identifier,
            engine_version="3.5.1",
            publicly_accessible=True,
            replication_instance_identifier=self.config.dms_replication_instance_name,
            resource_identifier=self.config.dms_replication_instance_name,
            multi_az=False,
            allocated_storage=20,
            vpc_security_group_ids=[
                self.db_security_group.attr_group_id,
            ],
        )
        self.dms_replication_instance.add_depends_on(self.db_instance)
        self.dms_replication_instance.add_depends_on(self.dms_subnet_group)

        # ref: https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_dms/CfnEndpoint.html
        self.dms_postgres_source_endpoint = dms.CfnEndpoint(
            self,
            "PostgresSourceEndpoint",
            endpoint_identifier=self.config.dms_postgres_source_endpoint_name,
            endpoint_type="source",
            engine_name="postgres",
            ssl_mode="require",
            server_name=self.db_instance.attr_endpoint_address,
            port=5432,
            database_name="postgres",
            username=self.config.username,
            password=self.config.password,
        )
        self.dms_postgres_source_endpoint.add_depends_on(self.db_instance)
        self.dms_postgres_source_endpoint.add_depends_on(self.dms_subnet_group)

        # ref: https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_dms/CfnEndpoint.html
        self.dms_s3_target_endpoint = dms.CfnEndpoint(
            self,
            "S3TargetEndpoint",
            endpoint_identifier=self.config.dms_s3_target_endpoint_name,
            endpoint_type="target",
            engine_name="s3",
            s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                bucket_name=s3paths.s3dir_dms_output_database.bucket,
                bucket_folder=s3paths.s3dir_dms_output_database.key,
                data_format="parquet",
                date_partition_enabled=True,
                date_partition_sequence="YYYYMMDDHH",
                date_partition_delimiter="SLASH",
                service_access_role_arn=self.dms_s3_endpoint_role.role_arn,
            ),
        )
        self.dms_s3_target_endpoint.add_depends_on(self.db_instance)
        self.dms_s3_target_endpoint.add_depends_on(self.dms_subnet_group)

    def declare_glue_catalog(self):
        # ref: https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_glue/CfnDatabase.html
        self.glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.config.aws_account_id,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self.config.glue_database,
            ),
        )

    def declare_glue_job(self):
        default_arguments = {
            "--datalake-formats": "hudi",
            "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false",
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--enable-job-insights": "false",
            "--enable-glue-datacatalog": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--job-bookmark-option": "job-bookmark-disable",
            "--job-language": "python",
            "--spark-event-logs-path": f"s3://{self.config.s3_bucket_glue_assets}/sparkHistoryLogs/",
            "--TempDir": f"s3://{self.config.s3_bucket_glue_assets}/temporary/",
            "--additional-python-modules": "boto_session_manager==1.5.3,s3pathlib==2.0.1",
        }

        self.glue_job_initial_load = glue.CfnJob(
            self,
            "GlueJobInitialLoad",
            name=self.config.glue_job_name_initial_load,
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=s3paths.s3path_initial_load_glue_script.uri,
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            max_retries=0,
            timeout=60,
            default_arguments={
                **default_arguments,
                "--S3URI_DMS_OUTPUT_DATABASE": s3paths.s3dir_dms_output_database.uri,
                "--S3URI_DATABASE": s3paths.s3dir_database.uri,
                "--DATABASE_NAME": self.config.glue_database,
                "--CODE_ETAG": s3paths.s3path_initial_load_glue_script.etag,
            },
        )

        self.glue_job_incremental = glue.CfnJob(
            self,
            "GlueJobIncremental",
            name=self.config.glue_job_name_incremental,
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=s3paths.s3path_incremental_glue_script.uri,
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            max_retries=0,
            timeout=60,
            default_arguments={
                **default_arguments,
                "--S3URI_DMS_OUTPUT_DATABASE": s3paths.s3dir_dms_output_database.uri,
                "--S3URI_DATABASE": s3paths.s3dir_database.uri,
                "--S3URI_INCREMENTAL_GLUE_JOB_INPUT": s3paths.s3dir_incremental_glue_job_input.uri,
                "--DATABASE_NAME": self.config.glue_database,
                "--CODE_ETAG": s3paths.s3path_incremental_glue_script.etag,
            },
        )


def pre_app_synth():
    s3paths.s3path_initial_load_glue_script.write_text(
        paths.path_glue_script_initial_load.read_text(),
        content_type="text/plain",
    )

    s3paths.s3path_incremental_glue_script.write_text(
        paths.path_glue_script_incremental.read_text(),
        content_type="text/plain",
    )


@dataclasses.dataclass
class ResourceActivationConfig:
    declare_s3_bucket: bool = dataclasses.field(default=False)
    declare_iam_role: bool = dataclasses.field(default=False)
    declare_rds_database: bool = dataclasses.field(default=False)
    declare_dms: bool = dataclasses.field(default=False)
    declare_glue_catalog: bool = dataclasses.field(default=False)
    declare_glue_job: bool = dataclasses.field(default=False)

    @classmethod
    def read(cls):
        if paths.path_cdk_stack_resource_activation_config_json.exists():
            return cls(
                **json.loads(
                    paths.path_cdk_stack_resource_activation_config_json.read_text()
                )
            )
        else:
            return cls()

    def write(self):
        paths.path_cdk_stack_resource_activation_config_json.write_text(
            json.dumps(dataclasses.asdict(self), indent=4)
        )


def create_stack(app: cdk.App) -> Stack:
    stack = Stack(
        app,
        construct_id=f"RDStoDataLakeStack",
        stack_name=config.cloudformation_stack_name,
        config=config,
    )
    return stack


def app_synth():
    resource_activation_config = ResourceActivationConfig.read()
    pre_app_synth()

    app = cdk.App()
    stack = create_stack(app)

    if resource_activation_config.declare_s3_bucket:
        stack.declare_s3_bucket()
    if resource_activation_config.declare_iam_role:
        stack.declare_iam_role()
    if resource_activation_config.declare_rds_database:
        stack.declare_rds_database()
    if resource_activation_config.declare_dms:
        stack.declare_dms()
    if resource_activation_config.declare_glue_catalog:
        stack.declare_glue_catalog()
    if resource_activation_config.declare_glue_job:
        stack.declare_glue_job()

    app.synth()
