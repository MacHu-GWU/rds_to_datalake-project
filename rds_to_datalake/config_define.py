# -*- coding: utf-8 -*-

"""
Project level configuration.
"""

import typing as T
import dataclasses
from boto_session_manager import BotoSesManager

from .compat import cached_property


@dataclasses.dataclass
class Config:
    """
    Project level configuration data model.

    :param app_name: app name, common prefix for all resources
    :param aws_profile: AWS cli profile for this project
    :param glue_database: glue catalog database name
    :param glue_table: glue catalog table name
    """

    app_name: str
    aws_profile: str
    vpc_id: str
    subnet_id_list: T.List[str]
    authorized_ip_list: T.List[str]
    port: int
    database: str
    username: str
    password: str

    @cached_property
    def bsm(self) -> BotoSesManager:
        return BotoSesManager(profile_name=self.aws_profile)

    @cached_property
    def aws_account_id(self) -> str:
        return self.bsm.aws_account_id

    @cached_property
    def aws_region(self) -> str:
        return self.bsm.aws_region

    @property
    def app_name_slug(self) -> str:
        return self.app_name.replace("_", "-")

    @property
    def app_name_snake(self) -> str:
        return self.app_name.replace("-", "_")

    @property
    def dms_vpc_role_name(self) -> str:
        return f"dms-vpc-role"

    @property
    def dms_cloudwatch_logs_role_name(self) -> str:
        return f"dms-cloudwatch-logs-role"

    @property
    def dms_rds_postgres_endpoint_role_name(self) -> str:
        return f"{self.app_name}-rds-postgres-endpoint-role"

    @property
    def dms_s3_endpoint_role_name(self) -> str:
        return f"{self.app_name}-s3-endpoint-role"

    @property
    def lambda_role_name(self) -> str:
        """
        AWS Lambda Function IAM role name (name only)
        """
        return f"{self.app_name}-lambda-role"

    @property
    def glue_role_name(self) -> str:
        """
        AWS Glue Job IAM role name (name only)
        """
        return f"{self.app_name}-glue-role"

    @property
    def dms_rds_postgres_endpoint_role_arn(self) -> str:
        return f"arn:aws:iam::{self.aws_account_id}:role/{self.dms_rds_postgres_endpoint_role_name}"

    @property
    def dms_s3_endpoint_role_arn(self) -> str:
        return (
            f"arn:aws:iam::{self.aws_account_id}:role/{self.dms_s3_endpoint_role_name}"
        )

    @property
    def lambda_role_arn(self) -> str:
        return f"arn:aws:iam::{self.aws_account_id}:role/{self.lambda_role_name}"

    @property
    def glue_role_arn(self) -> str:
        return f"arn:aws:iam::{self.aws_account_id}:role/{self.glue_role_name}"

    @property
    def cloudformation_stack_name(self) -> str:
        return self.app_name_slug

    @property
    def db_subnet_group_name(self) -> str:
        return f"{self.app_name}-db-subnet-group"

    @property
    def db_parameter_group_name(self) -> str:
        return f"{self.app_name_slug}-db-parameter-group"

    @property
    def db_security_group_name(self) -> str:
        return f"{self.app_name}-db-security-group"

    @property
    def db_instance_identifier(self) -> str:
        return f"{self.app_name_slug}-db"

    @property
    def db_instance_host_output_id(self) -> str:
        return f"DBInstanceHost"

    @property
    def dms_subnet_group_name(self) -> str:
        return f"{self.app_name}-dms-subnet-group"

    @property
    def dms_replication_instance_name(self) -> str:
        return self.app_name_slug

    @property
    def dms_replication_instance_arn(self) -> str:
        return f"arn:aws:dms:{self.aws_region}:{self.aws_account_id}:rep:{self.dms_replication_instance_name}"

    @property
    def dms_postgres_source_endpoint_name(self) -> str:
        return f"{self.app_name_slug}-postgres-source-endpoint"

    @property
    def dms_s3_target_endpoint_name(self) -> str:
        return f"{self.app_name_slug}-s3-target-endpoint"

    @property
    def dms_replication_task_name(self) -> str:
        return f"{self.app_name_slug}-replication-task"

    @property
    def lambda_function_name_dynamodb_stream_consumer(self) -> str:
        return f"{self.app_name}_dynamodb_stream_consumer"

    @property
    def glue_database(self) -> str:
        return self.app_name_snake

    @property
    def glue_job_name_initial_load(self) -> str:
        return f"{self.app_name}_initial_load"

    @property
    def glue_job_name_incremental(self) -> str:
        return f"{self.app_name}_incremental"

    @property
    def s3_bucket_artifacts(self) -> str:
        return f"{self.aws_account_id}-{self.aws_region}-artifacts"

    @property
    def s3_bucket_data(self) -> str:
        return f"{self.aws_account_id}-{self.aws_region}-data"

    @property
    def s3_bucket_glue_assets(self) -> str:
        return f"aws-glue-assets-{self.aws_account_id}-{self.aws_region}"
