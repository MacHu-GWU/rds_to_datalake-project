# -*- coding: utf-8 -*-

from .config_init import config
from .boto_ses import bsm
from .s3paths import s3dir_artifacts, s3dir_data
from .glue_job import delete_glue_job_if_exists
from .cdk_deploy import cdk_destroy, get_cloudformation_stack_console_url
from .db_orm import table_name_list


def cleanup():
    print("--- Clean up s3")
    print(f"clean up {s3dir_artifacts.uri}, preview at: {s3dir_artifacts.console_url}")
    s3dir_artifacts.delete()
    print(f"clean up {s3dir_data.uri}, preview at: {s3dir_data.console_url}")
    s3dir_data.delete()

    print("--- Clean up Glue Catalog")
    for table_name in table_name_list:
        print(f"clean up glue table {table_name!r}")
        delete_glue_job_if_exists(bsm.glue_client, table_name)

    print("--- Clean up CloudFormation")
    url = get_cloudformation_stack_console_url(
        aws_region=config.aws_region,
        stack_name=config.cloudformation_stack_name,
    )
    print(
        f"clean up cloudformation stack {config.cloudformation_stack_name!r}, preview at: {url}"
    )
    cdk_destroy()
    s3dir_artifacts.delete()
    s3dir_data.delete()
