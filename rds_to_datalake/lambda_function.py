# -*- coding: utf-8 -*-

"""
Lambda function related functions.
"""

import typing as T


def get_lambda_function_console_url(
    aws_region: str,
    function_name: str,
) -> str:
    return (
        f"https://{aws_region}.console.aws.amazon.com"
        f"/lambda/home?region={aws_region}#/functions"
        f"/{function_name}?tab=code"
    )


def get_lambda_function(
    lambda_client,
    function_name: str,
) -> T.Optional[dict]:
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/get_function.html
    try:
        response = lambda_client.get_function(
            FunctionName=function_name,
        )
        return response["Configuration"]
    except Exception as e:
        if "not found" in str(e).lower():
            return None
        else:
            raise NotImplementedError


def delete_function(
    lambda_client,
    function_name: str,
):
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/delete_function.html
    lambda_client.delete_function(
        FunctionName=function_name,
    )


def delete_function_if_exists(
    lambda_client,
    function_name: str,
):
    if get_lambda_function(lambda_client, function_name) is not None:
        delete_function(lambda_client, function_name)
