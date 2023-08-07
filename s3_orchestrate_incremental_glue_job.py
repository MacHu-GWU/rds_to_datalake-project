# -*- coding: utf-8 -*-

import time
from rds_to_datalake.glue_job import run_incremental_glue_job

while 1:
    run_incremental_glue_job()
    print("waiting 60 seconds ...")
    time.sleep(60)

# from datetime import datetime, timezone
# from rich import print as rprint
# from rds_to_datalake.boto_ses import bsm
# from rds_to_datalake import s3paths
# from rds_to_datalake.config_init import config
# from rds_to_datalake.incremental_load_orchestration import CDCTracker
#
#
# cdc_tracker = CDCTracker.read(
#     bsm=bsm,
#     s3path_tracker=s3paths.s3path_incremental_glue_job_tracker,
#     s3dir_glue_job_input=s3paths.s3dir_incremental_glue_job_input,
#     s3dir_dms_output_database=s3paths.s3dir_dms_output_database,
#     glue_job_name="hello",
#     epoch_processed_datetime=datetime(2023, 1, 1, tzinfo=timezone.utc),
# )
# cdc_tracker.run_glue_job(bsm=bsm)
# # print(cdc_tracker)
# cdc_tracker.try_to_run_glue_job(bsm=bsm)

# print(cdc_tracker.s3path_tracker.console_url)
# for table_tracker in cdc_tracker.table_tracker_list:
#     rprint(table_tracker.get_todo(bsm=bsm, s3dir_dms_output_database=s3paths.s3dir_dms_output_database))
# while 1:
#     run_incremental_glue_job(epoch_processed_partition="year=2023/month=07/day=31/hour=21/minute=38")
#     print("waiting 60 seconds ...")
#     time.sleep(60)
