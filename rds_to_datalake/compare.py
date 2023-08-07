# -*- coding: utf-8 -*-

import typing as T

import sqlalchemy as sa
from rich import print as rprint

from .config_init import config
from .db_connect import create_engine_for_this_project
from .db_orm import table_name_list, get_table_def
from .athena import run_athena_query


T_RECORDS = T.List[T.Dict[str, T.Any]]


def read_from_rds_table(
    engine: sa.engine.Engine,
    table_name: str,
) -> T_RECORDS:
    table = get_table_def(table_name)
    rows = list()
    with engine.connect() as conn:
        stmt = sa.select(table).order_by(table.c.id)
        for row in conn.execute(stmt).mappings():
            rows.append(dict(row))
    return rows


def read_from_hudi_table(
    table_name: str,
) -> T_RECORDS:
    df = run_athena_query(
        database=config.glue_database,
        sql=f"SELECT * FROM {config.glue_database}.{table_name} ORDER BY id",
        verbose=False,
    )
    columns_to_drop = [
        "create_year",
        "create_month",
        "create_day",
        "create_hour",
        "create_minute",
    ]
    for column in df.columns:
        if column.startswith("_hoodie"):
            columns_to_drop.append(column)
    rows = df.drop(columns_to_drop).to_dicts()
    return rows


def compare_table(engine: sa.engine.Engine, table_name: str):
    print(f"--- Compare table {table_name!r} ---")
    rds_rows = read_from_rds_table(engine, table_name)
    dl_rows = read_from_hudi_table(table_name)
    n_rds_rows = len(rds_rows)
    n_dl_rows = len(dl_rows)
    print(f"n_rds_rows: {n_rds_rows}")
    print(f"n_dl_rows: {n_dl_rows}")
    if n_rds_rows != n_dl_rows:
        raise ValueError("n_rds_rows != n_dl_rows")
    else:
        print("NICE! n_rds_rows == n_dl_rows")

    # print first 10 rows that are different
    is_same = True
    diff_count = 0
    for rds_row, dl_row in zip(rds_rows, dl_rows):
        if rds_row != dl_row:
            is_same = False
            diff_count += 1
            if diff_count <= 10:
                print(f"rds_row: {rds_row}")
                print(f"dl_row: {dl_row}")

    if is_same is True:
        print("NICE! The data in rds and hudi are exactly the same.")
    else:
        print("OPS! The data in rds and hudi are not the same.")


def compare():
    """
    Compare the data in RDS and Hudi, see if they are exactly the same.
    """
    engine = create_engine_for_this_project()
    for table_name in table_name_list:
        compare_table(engine, table_name)
