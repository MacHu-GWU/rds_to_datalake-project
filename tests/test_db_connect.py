# -*- coding: utf-8 -*-

from rds_to_datalake.db_connect import create_engine_for_this_project


def test_create_engine_for_this_proejct():
    create_engine_for_this_project()


if __name__ == "__main__":
    from rds_to_datalake.tests.helper import run_cov_test

    run_cov_test(__file__, "rds_to_datalake.db_connect")
