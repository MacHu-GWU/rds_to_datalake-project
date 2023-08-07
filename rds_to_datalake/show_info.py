# -*- coding: utf-8 -*-

from . import s3paths
from .config_init import config
from .cdk_deploy import get_cloudformation_stack_console_url
from .lambda_function import get_lambda_function_console_url
from .glue_catalog import get_glue_database_console_url
from .glue_job import get_glue_job_console_url

def show_info():
    print("------ S3 info")
    print(f"s3dir_artifacts: {s3paths.s3dir_artifacts.console_url}")
    print(f"s3dir_data: {s3paths.s3dir_data.console_url}")
    print(f"s3dir_glue_artifacts: {s3paths.s3dir_glue_artifacts.console_url}")

    print(f"s3dir_database: {s3paths.s3dir_database.console_url}")
    print(f"s3dir_athena_result: {s3paths.s3dir_athena_result.console_url}")
    print(f"s3dir_dms_output_database: {s3paths.s3dir_dms_output_database.console_url}")
    print(f"s3dir_incremental_glue_job_input: {s3paths.s3dir_incremental_glue_job_input.console_url}")
    print(f"s3path_incremental_glue_job_tracker: {s3paths.s3path_incremental_glue_job_tracker.console_url}")
    print(f"s3dir_database: {s3paths.s3dir_database.console_url}")

    print("------ CloudFormation")
    url = get_cloudformation_stack_console_url(
        aws_region=config.aws_region,
        stack_name=config.cloudformation_stack_name,
    )
    print(f"cloudformation stack {config.cloudformation_stack_name!r}: {url}")

    print("------ Lambda Functions")
    function_name_list = [
    ]
    for function_name in function_name_list:
        url = get_lambda_function_console_url(
            aws_region=config.aws_region,
            function_name=function_name,
        )
        print(f"lambda function {function_name!r}: {url}")

    print("------ Glue Catalog")
    url = get_glue_database_console_url(
        aws_region=config.aws_region,
        database=config.glue_database,
    )
    print(f"glue database {config.glue_database!r}: {url}")

    print("------ Glue Jobs")
    url = get_glue_job_console_url(
        aws_region=config.aws_region,
        job_name=config.glue_job_name_initial_load,
    )
    print(f"glue job {config.glue_job_name_initial_load!r}: {url}")

    url = get_glue_job_console_url(
        aws_region=config.aws_region,
        job_name=config.glue_job_name_incremental,
    )
    print(f"glue job {config.glue_job_name_incremental!r}: {url}")
