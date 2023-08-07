# -*- coding: utf-8 -*-

"""
Incremental load orchestration logics.

[CN]

这个模块实现了每隔一段时间将最新的 Incremental Data Load 到 Hudi Table 中的 Cron Job 逻辑.
"""

import typing as T
import json
import enum
import dataclasses
from datetime import datetime, timedelta, timezone
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager


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
# CDC Data ETL progress tracker
# ------------------------------------------------------------------------------
class JobRunStateEnum(enum.Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    WAITING = "WAITING"


def datetime_to_s3_key(dt: datetime) -> str:
    """
    For example, in this project, the cdc data before
    ``datetime(2023, 1, 7, 8, 30, 15, 123000)`` will be stored at
    ``s3://bucket/prefix/table/2023/01/07/08/20230107-083015123.parquet``.
    We want to encode the s3 key based on the datetime.
    """
    return dt.strftime("%Y/%m/%d/%H/%Y%m%d-%H%M%S%f")[:-3]


def filename_to_datetime(filename: str) -> datetime:
    """
    In this project ``20230107-083015123.parquet`` file stores the cdc data
    before ``datetime(2023, 1, 7, 8, 30, 15, 123000)``. We want to parse the
    datetime from the file name.
    """
    return datetime.strptime(filename, "%Y%m%d-%H%M%S%f").replace(tzinfo=timezone.utc)


table_name_list = [
    "accounts",
    "transactions",
]

max_incremental_interval = 3600 * 24 * 30 * 12 * 999  # seconds
max_incremental_files = 2


@dataclasses.dataclass
class TableTracker:
    """
    Represent the incremental data processing progress of the table.

    :param table: table name
    :param epoch_processed_time_str: where the incremental data commit time start from.
    :param last_processed_time_str: the last processed commit time string in ISO format
    :param next_processed_time_str: the next processed commit time string in ISO format
    """

    table: str = dataclasses.field()
    epoch_processed_time_str: str = dataclasses.field()
    last_processed_time_str: T.Optional[str] = dataclasses.field(default=None)
    next_processed_time_str: T.Optional[str] = dataclasses.field(default=None)

    @property
    def last_processed_datetime(self) -> datetime:
        return datetime.fromisoformat(self.last_processed_time_str)

    @property
    def next_processed_datetime(self) -> datetime:
        return datetime.fromisoformat(self.next_processed_time_str)

    @property
    def last_processed_datetime_plus_1ms(self) -> datetime:
        return self.last_processed_datetime + timedelta(milliseconds=1)

    def get_todo(
        self,
        bsm: BotoSesManager,
        s3dir_dms_output_database: S3Path,
    ) -> T.Tuple[T.List[S3Path], datetime]:
        s3dir_table = s3dir_dms_output_database.joinpath("public", self.table).to_dir()
        last_processed_datetime_plus_1ms = self.last_processed_datetime_plus_1ms
        s3path_list = (
            s3dir_table.iter_objects(
                start_after=s3dir_table.joinpath(
                    datetime_to_s3_key(last_processed_datetime_plus_1ms),
                ).key,
                bsm=bsm,
            )
            .filter(
                lambda s3path: "/LOAD" not in s3path.uri,  #
                lambda s3path: (
                    filename_to_datetime(s3path.fname)
                    - last_processed_datetime_plus_1ms
                ).total_seconds()
                <= max_incremental_interval,
            )
            .all()
        )
        # print(s3path_list)
        if len(s3path_list) == 0:
            next_processed_datetime = last_processed_datetime_plus_1ms + timedelta(
                seconds=max_incremental_interval
            )
        else:
            s3path_list = s3path_list[:max_incremental_files]
            next_processed_datetime = filename_to_datetime(s3path_list[-1].fname)
        return s3path_list, next_processed_datetime


@dataclasses.dataclass
class CDCTracker:
    """
    CDC Glue Job Orchestration logic.

    :param s3path_tracker: where you store the cdc tracker data.
    :param s3dir_glue_job_input: where you store the glue job input parameters
        the folder structure looks like ``${reverse_sequence_id}-${sequence_id}``,
        the sequence id starts from 1, 2, ...::

        ...
        ${s3dir_glue_job_input}/999999997-000000003.json
        ${s3dir_glue_job_input}/999999998-000000002.json
        ${s3dir_glue_job_input}/999999999-000000001.json

    :param s3dir_dms_output_database: where you store the DMS initial load
        and incremental data output::

        ${s3dir_dms_output_database}/LOAD00000001.parquet
        ${s3dir_dms_output_database}/LOAD00000002.parquet
        ${s3dir_dms_output_database}/...
        ${s3dir_dms_output_database}/YYYY/MM/DD/HH/YYYYMMDD-HHmmssfff.parquet
        ${s3dir_dms_output_database}/YYYY/MM/DD/HH/YYYYMMDD-HHmmssfff.parquet
        ${s3dir_dms_output_database}/YYYY/MM/DD/HH/YYYYMMDD-HHmmssfff.parquet
        ${s3dir_dms_output_database}/...

    :param glue_job_name: the incremental glue job name.

    :param last_glue_job_run_id: the last glue job run id
    :param last_glue_job_run_sequence_id: the last glue job run sequence id
    :param ready_to_run_next_glue_job: whether the next glue job is ready to run.
        basically if the last glue job is not succeeded, failed, stopped, then
        it is NOT ready.
    """

    # static attributes
    s3path_tracker: S3Path = dataclasses.field()
    s3dir_glue_job_input: S3Path = dataclasses.field()
    s3dir_dms_output_database: S3Path = dataclasses.field()
    glue_job_name: str = dataclasses.field()

    table_tracker_list: T.List[TableTracker] = dataclasses.field(default_factory=list)

    # dynamic attributes
    last_glue_job_run_id: T.Optional[str] = dataclasses.field(default=None)
    last_glue_job_run_sequence_id: T.Optional[int] = dataclasses.field(default=None)
    ready_to_run_next_glue_job: T.Optional[bool] = dataclasses.field(default=None)

    @classmethod
    def read(
        cls,
        bsm: BotoSesManager,
        s3path_tracker: S3Path,
        s3dir_glue_job_input: S3Path,
        s3dir_dms_output_database: S3Path,
        glue_job_name: str,
        epoch_processed_datetime: datetime,
    ):
        """
        Read the tracker data from s3. If not exists, create a new one with
        initial value.
        """
        # set initial value if tracker not exists
        if s3path_tracker.exists(bsm=bsm) is False:
            tracker = cls(
                s3path_tracker=s3path_tracker,
                s3dir_glue_job_input=s3dir_glue_job_input,
                s3dir_dms_output_database=s3dir_dms_output_database,
                glue_job_name=glue_job_name,
                table_tracker_list=[
                    TableTracker(
                        table=table,
                        epoch_processed_time_str=epoch_processed_datetime.isoformat(),
                        last_processed_time_str=epoch_processed_datetime.isoformat(),
                        next_processed_time_str=None,
                    )
                    for table in table_name_list
                ],
                last_glue_job_run_id=None,
                last_glue_job_run_sequence_id=0,
                ready_to_run_next_glue_job=True,
            )
            tracker.write(bsm=bsm)
            return tracker
        # read from s3 if tracker exists
        else:
            data = json.loads(s3path_tracker.read_text(bsm=bsm))
            return cls(
                s3path_tracker=s3path_tracker,
                s3dir_glue_job_input=s3dir_glue_job_input,
                s3dir_dms_output_database=s3dir_dms_output_database,
                glue_job_name=glue_job_name,
                table_tracker_list=[
                    TableTracker(**dct) for dct in data["table_tracker_list"]
                ],
                last_glue_job_run_id=data["last_glue_job_run_id"],
                last_glue_job_run_sequence_id=data["last_glue_job_run_sequence_id"],
                ready_to_run_next_glue_job=data["ready_to_run_next_glue_job"],
            )

    def write(
        self,
        bsm: BotoSesManager,
    ):
        """
        Write the tracker data to s3.
        """
        self.s3path_tracker.write_text(
            json.dumps(
                {
                    "table_tracker_list": [
                        dataclasses.asdict(table_tracker)
                        for table_tracker in self.table_tracker_list
                    ],
                    "last_glue_job_run_id": self.last_glue_job_run_id,
                    "last_glue_job_run_sequence_id": self.last_glue_job_run_sequence_id,
                    "ready_to_run_next_glue_job": self.ready_to_run_next_glue_job,
                },
                indent=4,
            ),
            content_type="application/json",
            bsm=bsm,
        )

    def get_glue_job_input_s3path(self, sequence_id) -> S3Path:
        """
        Find the s3path of the glue job input file where the glue job can
        read the input data from.

        If the glue job run sequence id is 3, then the file name will be
        ``999999997-000000003.json``. This naming convention can return the
        latest glue job input parameter file first when we list the s3 directory.
        """
        filename = (
            f"{str(1000000000 - sequence_id).zfill(9)}"
            f"-{str(sequence_id).zfill(9)}.json"
        )
        return self.s3dir_glue_job_input.joinpath(filename)

    @property
    def last_glue_job_input_s3path(self) -> S3Path:
        return self.get_glue_job_input_s3path(
            sequence_id=self.last_glue_job_run_sequence_id,
        )

    @property
    def next_glue_job_input_s3path(self) -> S3Path:
        return self.get_glue_job_input_s3path(
            sequence_id=self.last_glue_job_run_sequence_id + 1,
        )

    def run_glue_job(self, bsm: BotoSesManager):
        print("prepare the glue job input data.")
        glue_job_input = GlueJobInput()
        for table_tracker in self.table_tracker_list:
            s3path_list, next_processed_datetime = table_tracker.get_todo(
                bsm=bsm,
                s3dir_dms_output_database=self.s3dir_dms_output_database,
            )
            table_tracker.next_processed_time_str = next_processed_datetime.isoformat()

            glue_job_input.todo_list.append(
                PerTableTodo(
                    table=table_tracker.table,
                    start_after=table_tracker.last_processed_datetime_plus_1ms.isoformat(),
                    end_until=next_processed_datetime.isoformat(),
                    s3uri_list=[s3path.uri for s3path in s3path_list],
                )
            )

        s3path_glue_job_input = self.next_glue_job_input_s3path
        print(f"write glue job input data to s3: {s3path_glue_job_input.uri}")
        glue_job_input.write(
            s3_client=bsm.s3_client,
            bucket=s3path_glue_job_input.bucket,
            key=s3path_glue_job_input.key,
        )

        # start glue job run
        # Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/start_job_run.html
        print("start glue job run.")
        try:
            res = bsm.glue_client.start_job_run(
                JobName=self.glue_job_name,
                Arguments={
                    "--S3URI_INCREMENTAL_GLUE_JOB_INPUT": s3path_glue_job_input.uri,
                },
            )
            job_run_id = res["JobRunId"]
            print(f"job run id = {job_run_id}")
            self.last_glue_job_run_id = job_run_id
            self.last_glue_job_run_sequence_id += 1
            self.ready_to_run_next_glue_job = False
            self.write(bsm=bsm)
            return True
        except Exception as e:
            if "concurrent runs exceeded" in str(e).lower():
                return False
            else:
                raise NotImplementedError(
                    f"didn't implement the error handling logic for exception: {e!r}"
                )

    def try_to_run_glue_job(self, bsm: BotoSesManager) -> bool:
        """
        Check the status of the last glue job run, if it is finished, then
        update the tracker and run a new glue job.

        :return: a boolean flag to indicate if it runs the glue job,
        """
        print("try to run incremental glue job.")
        if self.ready_to_run_next_glue_job:
            return self.run_glue_job(bsm=bsm)
        else:
            if self.last_glue_job_run_id is None:
                raise ValueError

            # get job run status
            # Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/get_job_run.html
            print("there is a running incremental glue job, check the status.")
            res = bsm.glue_client.get_job_run(
                JobName=self.glue_job_name,
                RunId=self.last_glue_job_run_id,
            )
            state = res["JobRun"]["JobRunState"]

            # if finished (succeeded or failed), update the tracker and run another job
            if state in [
                JobRunStateEnum.STOPPED.value,
                JobRunStateEnum.SUCCEEDED.value,
                JobRunStateEnum.FAILED.value,
                JobRunStateEnum.TIMEOUT.value,
                JobRunStateEnum.ERROR.value,
            ]:
                for table_tracker in self.table_tracker_list:
                    table_tracker.last_processed_time_str = (
                        table_tracker.next_processed_time_str
                    )
                    table_tracker.next_processed_time_str = None
                self.ready_to_run_next_glue_job = True
                print(
                    f"previous glue job finished, "
                    f"status = {state!r}, run another one."
                )
                return self.run_glue_job(bsm=bsm)
            else:
                print(
                    f"there is a running incremental glue job, "
                    f"status = {state!r}, do nothing."
                )
                return False
