# -*- coding: utf-8 -*-

import typing as T
import uuid
import textwrap
import polars as pl

from ..waiter import Waiter


def split_s3_uri(
    s3_uri: str,
) -> T.Tuple[str, str]:
    """
    Split AWS S3 URI, returns bucket and key.

    :param s3_uri: example, ``"s3://my-bucket/my-folder/data.json"``

    :return: example, ``("my-bucket", "my-folder/data.json")``
    """
    parts = s3_uri.split("/", 3)
    bucket = parts[2]
    key = parts[3]
    return bucket, key


def run_athena_query(
    athena_client,
    s3_client,
    s3uri_result: str,
    database: str,
    sql: str,
    verbose: bool = True,
    catalog: T.Optional[str] = None,
    cache_expire: int = 0, # in minutes
) -> pl.DataFrame:
    """
    Run athena query and get the result as a polars.DataFrame.
    """
    # resolve arguments
    if s3uri_result.endswith("/") is False:
        s3uri_result = s3uri_result + "/"
    s3uri_result = f"{s3uri_result}{uuid.uuid4().hex}/"
    if catalog is None:
        catalog = "AwsDataCatalog"
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1]

    # ref: https://docs.aws.amazon.com/athena/latest/ug/unload.html
    final_sql = textwrap.dedent(f"""
    UNLOAD ({sql}) 
    TO '{s3uri_result}' 
    WITH ( format = 'parquet' )
    """)

    # run query
    if verbose:
        print(f"raw query:")
        print(sql)
        print(f"final athena query:")
        print(final_sql)

    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/start_query_execution.html
    kwargs = dict(
        QueryString=final_sql,
        QueryExecutionContext=dict(
            Catalog=catalog,
            Database=database,
        ),
        ResultConfiguration=dict(
            OutputLocation=s3uri_result,
        ),
    )
    if cache_expire != 0:
        kwargs["ResultReuseConfiguration"] = {
            "ResultReuseByAgeConfiguration": {
                "Enabled": True,
                "MaxAgeInMinutes": cache_expire,
            }
        }

    response = athena_client.start_query_execution(**kwargs)
    exec_id = response["QueryExecutionId"]
    if verbose:
        print(f"query execution id: {exec_id}")

    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/get_query_execution.html
    for _ in Waiter(
        delays=1,
        timeout=10,
        verbose=verbose,
    ):
        response = athena_client.get_query_execution(
            QueryExecutionId=exec_id,
        )
        status = response["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            break
        elif status in ["FAILED", "CANCELLED"]:
            raise RuntimeError(f"status = {status}")
        else:
            pass

    # get query result
    if verbose:
        print("")

    s3uri = f"{s3uri_result}{exec_id}.csv"
    res = s3_client.get_object(
        Bucket=bucket,
    )
    s3path_athena_result = s3dir_athena_result.joinpath(f"{exec_id}.csv")
    with s3path_athena_result.open("rb") as f:
        df = pl.read_csv(f.read())
    return df