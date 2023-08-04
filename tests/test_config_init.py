# -*- coding: utf-8 -*-

from rds_to_datalake.config_init import config


def test():
    print(config)


if __name__ == "__main__":
    from rds_to_datalake.tests.helper import run_cov_test

    run_cov_test(__file__, "rds_to_datalake.config_init")
