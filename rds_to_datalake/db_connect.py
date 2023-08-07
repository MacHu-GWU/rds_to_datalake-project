# -*- coding: utf-8 -*-

import sqlalchemy as sa


def create_engine(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> sa.engine.Engine:
    return sa.create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}",
    )


def create_engine_for_this_project() -> sa.engine.Engine:
    from .config_init import config
    from .cdk_export import Outputs

    outputs = Outputs.read()

    engine = create_engine(
        host=outputs.db_instance_host,
        port=config.port,
        database=config.database,
        username=config.username,
        password=config.password,
    )
    with engine.connect() as conn:
        for row in conn.execute(
            sa.text("SELECT 1;")
        ):
            assert row[0] == 1

    return engine
