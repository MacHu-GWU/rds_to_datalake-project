# -*- coding: utf-8 -*-

"""
Ref: https://docs.sqlalchemy.org/en/20/orm/quickstart.html
"""

import typing as T
import sqlalchemy as sa
import sqlalchemy.orm as orm


class Base(orm.DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    email: orm.Mapped[str] = orm.mapped_column(sa.String())
    create_at: orm.Mapped[str] = orm.mapped_column(sa.String())
    update_at: orm.Mapped[str] = orm.mapped_column(sa.String())

    transactions: orm.Mapped[T.List["Transaction"]] = orm.relationship(
        back_populates="account",
        cascade="all, delete-orphan",
    )


class Transaction(Base):
    __tablename__ = "transactions"

    id: orm.Mapped[str] = orm.mapped_column(sa.String(), primary_key=True)
    account_id: orm.Mapped[str] = orm.mapped_column(sa.ForeignKey("accounts.id"))
    create_at: orm.Mapped[str] = orm.mapped_column(sa.String())
    update_at: orm.Mapped[str] = orm.mapped_column(sa.String())
    entity: orm.Mapped[str] = orm.mapped_column(sa.String())
    amount: orm.Mapped[int] = orm.mapped_column(sa.SMALLINT())
    is_credit: orm.Mapped[int] = orm.mapped_column(sa.SMALLINT())
    note: orm.Mapped[str] = orm.mapped_column(sa.String())

    account: orm.Mapped["Account"] = orm.relationship(back_populates="transactions")


transaction_create_at_index = sa.Index(
    "transaction_create_at_idx",
    Transaction.create_at,
)

table_name_list = list(Base.metadata.tables)
table_name_list.sort()

def get_table_def(table_name: str) -> sa.Table:
    return Base.metadata.tables[table_name]
