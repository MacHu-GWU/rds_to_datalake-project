# -*- coding: utf-8 -*-

# standard library
import typing as T
import sys
import json
import dataclasses

# third party library
import boto3
from s3pathlib import S3Path

# pyspark / AWS Glue stuff
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# ------------------------------------------------------------------------------
# create spark session
# ------------------------------------------------------------------------------
print("create spark session")
conf = (
    SparkConf()
    .setAppName("MyApp")
    .setAll(
        [
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
            ("spark.sql.hive.convertMetastoreParquet", "false"),
        ]
    )
)
spark_ses = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark_ctx = spark_ses.sparkContext
glue_ctx = GlueContext(spark_ctx)

# ------------------------------------------------------------------------------
# resolve job parameters
# ------------------------------------------------------------------------------
print("resolve job parameters")
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3URI_DMS_OUTPUT_DATABASE",
        "S3URI_DATABASE",
        "S3URI_INCREMENTAL_GLUE_JOB_INPUT",
        "DATABASE_NAME",
    ],
)
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

S3URI_DMS_OUTPUT_DATABASE = args["S3URI_DMS_OUTPUT_DATABASE"]
S3URI_DATABASE = args["S3URI_DATABASE"]
S3URI_INCREMENTAL_GLUE_JOB_INPUT = args["S3URI_INCREMENTAL_GLUE_JOB_INPUT"]
DATABASE_NAME = args["DATABASE_NAME"]

# ------------------------------------------------------------------------------
# create boto3 session
# ------------------------------------------------------------------------------
print("create boto3 session")
boto_ses = boto3.session.Session()
sts_client = boto_ses.client("sts")
aws_account_id = sts_client.get_caller_identity()["Account"]
aws_region = boto_ses.region_name

print(f"aws_account_id = {aws_account_id}")
print(f"aws_region = {aws_region}")


# ------------------------------------------------------------------------------
# Incremental Glue Job Input Data Model
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class PerTableTodo:
    table: str = dataclasses.field()
    start_after: str = dataclasses.field()
    end_until: str = dataclasses.field()
    s3uri_list: T.List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class GlueJobInput:
    todo_list: T.List[PerTableTodo] = dataclasses.field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict):
        data["todo_list"] = [PerTableTodo(**dct) for dct in data["todo_list"]]
        return cls(**data)

    def to_dict(self):
        return dataclasses.asdict(self)

    @classmethod
    def read(cls, s3_client, bucket: str, key: str):
        res = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(res["Body"].read().decode("utf-8"))
        return cls.from_dict(data)

    def write(self, s3_client, bucket: str, key: str):
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(self.to_dict(), indent=4),
            ContentType="application/json",
        )


# ------------------------------------------------------------------------------
# read glue job input data from s3
# ------------------------------------------------------------------------------
print("read glue job input data from s3")
# figure out where to read data and where to dump data
s3dir_dms_output_database = S3Path(S3URI_DATABASE)
s3dir_database = S3Path(S3URI_DATABASE)
s3path_incremental_glue_job_input = S3Path(S3URI_INCREMENTAL_GLUE_JOB_INPUT)

s3_client = boto_ses.client("s3")
glue_job_input = GlueJobInput.read(
    s3_client=s3_client,
    bucket=s3path_incremental_glue_job_input.bucket,
    key=s3path_incremental_glue_job_input.key,
)


# ------------------------------------------------------------------------------
# ETL Logics
# ------------------------------------------------------------------------------
def show_df(pdf, n: int = 3):
    pdf.show(n, vertical=True, truncate=False)


def show_df_details(pdf, name: str):
    print(name)
    pdf.printSchema()
    show_df(pdf)
    print(f"{name}.count() = {pdf.count()}")


def process_one_table(
    per_table_todo: PerTableTodo,
):
    # --------------------------------------------------------------------------
    # read initial load data
    # --------------------------------------------------------------------------
    print(f"read incremental data of table {per_table_todo.table!r}")
    if len(per_table_todo.s3uri_list) == 0:
        print("no incremental data to process, skip")

    pdf_incremental = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": per_table_todo.s3uri_list,
        },
        format="parquet",
    ).toDF()
    show_df_details(pdf_incremental, "pdf_incremental")

    # --------------------------------------------------------------------------
    # transform data
    # --------------------------------------------------------------------------
    print("transform data")
    # ------------------------------------------------------------------------------
    # only keep the latest version of each record
    # ------------------------------------------------------------------------------
    # add row_number column
    pdf_incremental_1 = pdf_incremental.withColumn(
        "row_number",
        F.row_number().over(
            Window.partitionBy("id").orderBy(F.col("update_at").desc())
        ),
    )
    pdf_incremental_2 = (
        pdf_incremental_1
        .filter(pdf_incremental_1.row_number == 1)
        .drop("row_number")
        .sort(pdf_incremental_1.update_at.desc())
    )
    show_df_details(pdf_incremental_2, "pdf_incremental_2")

    # ------------------------------------------------------------------------------
    # generate create_year, create_month, ..., create_minute columns
    # ------------------------------------------------------------------------------
    pdf_incremental_3 = (
        pdf_incremental_2.withColumn(
            "create_year",
            F.substring(pdf_incremental_2.create_at, 1, 4),
        )
        .withColumn(
            "create_month",
            F.substring(pdf_incremental_2.create_at, 6, 2),
        )
        .withColumn(
            "create_day",
            F.substring(pdf_incremental_2.create_at, 9, 2),
        )
        .withColumn(
            "create_hour",
            F.substring(pdf_incremental_2.create_at, 12, 2),
        )
        .withColumn(
            "create_minute",
            F.substring(pdf_incremental_2.create_at, 15, 2),
        )
        .drop("Op")
    )
    show_df_details(pdf_incremental_3, "pdf_incremental_3")

    # --------------------------------------------------------------------------
    # write data
    # --------------------------------------------------------------------------
    print("write data")
    database = DATABASE_NAME
    table = per_table_todo.table

    additional_options = {
        "hoodie.table.name": table,
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "update_at",
        "hoodie.datasource.write.partitionpath.field": "create_year,create_month,create_day,create_hour,create_minute",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": database,
        "hoodie.datasource.hive_sync.table": table,
        "hoodie.datasource.hive_sync.partition_fields": "create_year,create_month,create_day,create_hour,create_minute",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        "path": s3dir_database.joinpath(table).uri,
    }
    (
        pdf_incremental_3.write.format("hudi")
        .options(**additional_options)
        .mode("append")
        .save()
    )

for per_table_todo in glue_job_input.todo_list:
    process_one_table(per_table_todo)

job.commit()
