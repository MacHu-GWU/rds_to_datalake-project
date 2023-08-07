# -*- coding: utf-8 -*-

from rds_to_datalake.config_init import config


def test():
    _ = config
    _ = config.bsm
    _ = config.aws_account_id
    _ = config.aws_region
    _ = config.app_name_slug
    _ = config.app_name_snake
    _ = config.dms_vpc_role_name
    _ = config.dms_cloudwatch_logs_role_name
    _ = config.dms_rds_postgres_endpoint_role_name
    _ = config.dms_s3_endpoint_role_name
    _ = config.lambda_role_name
    _ = config.glue_role_name
    _ = config.dms_rds_postgres_endpoint_role_arn
    _ = config.dms_s3_endpoint_role_arn
    _ = config.lambda_role_arn
    _ = config.glue_role_arn
    _ = config.cloudformation_stack_name
    _ = config.db_subnet_group_name
    _ = config.db_parameter_group_name
    _ = config.db_security_group_name
    _ = config.db_instance_identifier
    _ = config.db_instance_host_output_id
    _ = config.dms_subnet_group_name
    _ = config.dms_replication_instance_name
    _ = config.dms_replication_instance_arn
    _ = config.dms_postgres_source_endpoint_name
    _ = config.dms_s3_target_endpoint_name
    _ = config.dms_replication_task_name
    _ = config.lambda_function_name_dynamodb_stream_consumer
    _ = config.glue_database
    _ = config.glue_job_name_initial_load
    _ = config.glue_job_name_incremental
    _ = config.s3_bucket_artifacts
    _ = config.s3_bucket_data
    _ = config.s3_bucket_glue_assets


if __name__ == "__main__":
    from rds_to_datalake.tests.helper import run_cov_test

    run_cov_test(__file__, "rds_to_datalake.config_init")
