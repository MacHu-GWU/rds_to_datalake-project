AWS Database Migration Service (DMS)
==============================================================================


Overview
------------------------------------------------------------------------------
Release in 2016

- Step 1: Create a replication instance
- Step 2: Specify source and target endpoints
- Step 3: Create a task and migrate data
- Step 4: Test replication
- Step 5: Clean up AWS DMS resources


如果你的 Postgres engine version 是 15.X, 那么你需要修改你的 Postgres RDS Instance 的 Parameter Group,

这个配置只是在你的 Migration Task 需要 Capture CDC change 的时候才需要启用. 你无需启用这个就能做 Migration Initial Load.
先创建一个 Parameter Group, 选择 postgres15 模版, 然后再 Parameters 中搜索, 找到 ``rds.logical_replication`` 并将其修改为 1. 回到RDS Instance, 选择 Modify, 然后选择 Immediate Apply the Change. 等待 DB Instance 的状态变为 Modifying 然后变为 running. 然后将其重启. 因为这个配置需要重启后才能生效.

`Setting up replication for AWS Database Migration Service <https://docs.aws.amazon.com/dms/latest/userguide/CHAP_GettingStarted.Replication.html>`_
`Working with AWS-managed PostgreSQL databases as a DMS source <https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html#CHAP_Source.PostgreSQL.RDSPostgreSQL>`_
`Creating a Task <https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.Creating.html>`_

S3 Target Endpoint 的进阶配置

    - 注意: 以下的所有配置要么是在创建 S3 Target Endpoint 的时候, 在 Endpoint Settings 里设置, 要么是在 Modify S3 Target Endpoint 的时候设置. 如果你是创建之后然后 Modify, 你要先确保没有ongoing 的 Migration Task 依赖于这个 S3 Target Endpoint. 如果有, 你需要先 Stop (不需要 Delete), 然后等 Task Stopped 之后再 Modify. 这里还要注意你在 Modify 的时候, 底下会有一个输入框让你输入 ``Extra connection attributes``, 这里面的 setting 会 override 之前的 key value setting, 请自己注意两者是否有冲突.
    - `Using data encryption, parquet files, and CDC on your Amazon S3 target <https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.EndpointSettings>`_: 这篇文档介绍了当用 S3 作为 Target Endpoint 时, 如何配置使用 parquet 作为导出格式. 你可以搜索 "Settings for using .parquet files to store S3 target objects" 这个副标题. 其中最关键的配置是 ``{"DataFormat": "parquet"}``.
    - `Using date-based folder partitioning <https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.DatePartitioning>`_: 这篇文档介绍了如何用 Commit time 作为 S3 导出的 partition. 注意这里最多支持按照 年, 月, 日, 小时 分区, 不支持分钟级的分区. 其中最关键的配置是 ``{"DatePartitionEnabled": true, "DatePartitionSequence": "YYYYMMDDHH", "DatePartitionDelimiter": "SLASH"}``.
- `Using Amazon S3 as a target for AWS Database Migration Service <https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html>`_: 这篇文档详细介绍了当使用 Amazon S3 作为 Target 的时候的所有需要注意的详细配置. 其中开头介绍了 S3 Output 的 Folder Structure.
    - 对于 initial load 的目录结构如下. 第一级目录事 database_schema_name, 第二级是 table_name, 里面放着的文件则是 LOAD00000001.csv (如果你用的其他格式, 例如 parquet, 那么后缀名就是 .parquet). 其中每一位数字都是16进制的字符, 一共有8位, 那么可以生成最多 16**8 = 2**24 = 4,194,304 个文件, 足够用了. 根据文件名可知, 就算没有 partition, 你可以用 ``S3.list_objects(start_from=".../LOAD")`` 来筛选出所有属于 initial load 的文件.

    .. code-block:: python

        database_schema_name/table_name/LOAD00000001.csv
        database_schema_name/table_name/LOAD00000002.csv
        ...
        database_schema_name/table_name/LOAD00000009.csv
        database_schema_name/table_name/LOAD0000000A.csv
        database_schema_name/table_name/LOAD0000000B.csv
        ...
        database_schema_name/table_name/LOAD0000000F.csv
        database_schema_name/table_name/LOAD00000010.csv
        database_schema_name/table_name/LOAD00000011.csv
        database_schema_name/table_name/LOAD00000012.csv
        ...
        database_schema_name/table_name/LOAD00000019.csv

    - 对于 cdc load 的目录结构如下. 如果你没有启用 partition, 那么所有的 cdc 文件也会被放在 ``database_schema_name/table_name/`` 下. 如果你启用了 partition 那么会被放在 partition 目录下, 例如 ``database_schema_name/table_name/yyyy/mm/dd/HH/``. 而文件名则是 ``yyyymmdd-HHMMSSfff.csv`` 的时间戳 (文件后缀名取决于你的 data format, 前面已经说过了, 这里不再重复), 其中小数点后精确到毫秒. 这个时间戳的含义是该文件中保存的是该时间戳代表的 commit time 之前的所有 cdc 数据, 其中包含 insert, update, delete. 换言之如果你的数据中有一个字段是 update_time 的话, 那么文件中所有数据的 update_time 必然小于等于这个文件名时间戳. 根据文件名可知, 就算没有 partition, 你可以用 ``S3.list_objects(start_from=".../2")`` 来筛选出所有属于 cdc 的文件.

    .. code-block:: python
    
        database_schema_name/table_name/yyyymmdd-HHMMSSfff.csv
        # example
        database_schema_name/table_name/2023-01-01-083015000.csv
        database_schema_name/table_name/2023-01-01-083020000.csv
        database_schema_name/table_name/2023-01-01-083025000.csv

`Migrating an Amazon RDS for Oracle Database to an Amazon S3 Data Lake <https://docs.aws.amazon.com/dms/latest/sbs/oracle-s3-data-lake.html>`_: 这篇文档详细介绍了将 AWS RDS for Oracle Database 中的数据导出到 S3 Data Lake 的详细步骤. 这个例子也适用于其他的 AWS RDS for MySQL / Postgres 等导出到 S3 Data Lake 的情况.
