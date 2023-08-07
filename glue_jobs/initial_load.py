# -*- coding: utf-8 -*-

# standard library
import sys

# third party library
import boto3
from s3pathlib import S3Path, context

# pyspark / AWS Glue stuff
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# ------------------------------------------------------------------------------
# create spark session
# ------------------------------------------------------------------------------
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
print("create spark session")
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3URI_DMS_OUTPUT_DATABASE",
        "S3URI_DATABASE",
        "DATABASE_NAME",
    ],
)
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

S3URI_DMS_OUTPUT_DATABASE = args["S3URI_DMS_OUTPUT_DATABASE"]
S3URI_DATABASE = args["S3URI_DATABASE"]
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
context.attach_boto_session(boto_ses)

# figure out where to read data and where to dump data
s3dir_dms_output_database = S3Path(S3URI_DMS_OUTPUT_DATABASE)
s3dir_database = S3Path(S3URI_DATABASE)

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
    s3dir_table: S3Path,
):
    # --------------------------------------------------------------------------
    # read initial load data
    # --------------------------------------------------------------------------
    print(f"read initial load data from {s3dir_table.uri}")
    initial_load_s3path_list = list()
    for s3path in s3dir_table.iter_objects(
        start_after=s3dir_table.joinpath("LOAD").key,
        recursive=False,
    ):
        if s3path.key.endswith(".parquet"):
            initial_load_s3path_list.append(s3path)

    n_files = len(initial_load_s3path_list)
    print(f"got {n_files} for initial load")
    print("preview first 3 files:")
    for s3path in initial_load_s3path_list[:3]:
        print(f"{s3path.uri}")

    if n_files == 0:
        print("no initial load file found, skip")
        return

    print("read data")
    pdf_initial = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [s3path.uri for s3path in initial_load_s3path_list],
            "recurse": False,
        },
        format="parquet",
    ).toDF()
    show_df_details(pdf_initial, "pdf_initial")

    # --------------------------------------------------------------------------
    # transform data
    # --------------------------------------------------------------------------
    print("transform data")
    # generate create_year, create_month, ..., create_minute columns
    pdf_initial_enriched = (
        pdf_initial.withColumn(
            "create_year",
            F.substring(pdf_initial.create_at, 1, 4),
        )
        .withColumn(
            "create_month",
            F.substring(pdf_initial.create_at, 6, 2),
        )
        .withColumn(
            "create_day",
            F.substring(pdf_initial.create_at, 9, 2),
        )
        .withColumn(
            "create_hour",
            F.substring(pdf_initial.create_at, 12, 2),
        )
        .withColumn(
            "create_minute",
            F.substring(pdf_initial.create_at, 15, 2),
        )
    )
    show_df_details(pdf_initial_enriched, "pdf_initial_enriched")

    # --------------------------------------------------------------------------
    # write data
    # --------------------------------------------------------------------------
    print("write data")
    database = DATABASE_NAME
    table = s3dir_table.basename

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
        pdf_initial_enriched.write.format("hudi")
        .options(**additional_options)
        .mode("overwrite")
        .save()
    )


s3dir_public = s3dir_dms_output_database.joinpath("public").to_dir()
print(f"scan tables in {s3dir_public.uri}")
for s3dir_table in s3dir_public.iterdir():
    process_one_table(s3dir_table)


job.commit()
