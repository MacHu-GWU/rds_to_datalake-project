# -*- coding: utf-8 -*-

"""
This script continuously ingest data into DynamoDB table
"""

import time
import random
from datetime import datetime, timezone

from faker import Faker
from fixa.timer import DateTimeTimer
from ordered_set import OrderedSet
import sqlalchemy as sa
import sqlalchemy.orm as orm
from rich import print as rprint

from rds_to_datalake.db_connect import create_engine_for_this_project
from rds_to_datalake.db_orm import Base, Account, Transaction

engine = create_engine_for_this_project()
Base.metadata.create_all(engine)


fake = Faker()


def get_utc_now():
    return datetime.utcnow().replace(tzinfo=timezone.utc)


digits = "0123456789"


def rnd_digits(n: int) -> str:
    return "".join([random.choice(digits) for _ in range(n)])


def rnd_account() -> str:
    return "-".join(
        [
            rnd_digits(3),
            rnd_digits(3),
            rnd_digits(4),
        ]
    )


with engine.connect() as conn:
    account_set = OrderedSet(
        [row[0] for row in conn.execute(sa.text("SELECT id FROM accounts"))]
    )
    transaction_set = OrderedSet(
        [row[0] for row in conn.execute(sa.text("SELECT id FROM transactions"))]
    )


def new_account():
    """
    Simulate an event that create a new account.
    """
    with orm.Session(engine) as session:
        account = Account(
            id=rnd_account(),
            email=fake.email(),
        )
        session.add(account)
        session.commit()
        account_set.add(account.id)


def update_account():
    with orm.Session(engine) as session:
        account_id = random.choice(account_set)
        account = session.get(Account, account_id)
        account.email = fake.email()
        session.commit()


def new_transaction():
    """
    Simulate an event that create a new transaction.
    """
    with orm.Session(engine) as session:
        account_id = random.choice(account_set)
        now = get_utc_now()
        transaction = Transaction(
            id=f"{account_id}={now.isoformat()}",
            account_id=account_id,
            create_at=now.isoformat(),
            update_at=now.isoformat(),
            entity=fake.company(),
            amount=random.randint(1, 1000),
            is_credit=random.randint(0, 1),
            note="\n".join(fake.paragraphs()),
        )
        session.add(transaction)
        session.commit()
        transaction_set.add(transaction.id)


def update_transaction():
    with orm.Session(engine) as session:
        transaction_id = random.choice(transaction_set)
        transaction_id = session.get(Transaction, transaction_id)

        now = get_utc_now()
        transaction_id.update_at = now.isoformat()
        transaction_id.note = "\n".join(fake.paragraphs())
        session.commit()


def delete_all():
    with orm.Session(engine) as session:
        session.query(Transaction).delete()
        session.query(Account).delete()
        session.commit()


def run_data_faker():
    # for debug only
    # new_account()
    # update_account()
    # new_transaction()
    # update_transaction()

    # create 1 initial account and transaction first
    new_account()
    new_transaction()

    ith = 0
    while 1:
    # for _ in range(100):
        ith += 1
        if random.randint(1, 100) <= 90:
            if random.randint(1, 100) <= 90:
                new_transaction()
                print(f"finished {ith} th 'new transaction' event")
            else:
                update_transaction()
                print(f"finished {ith} th 'update transaction' event")
        else:
            if random.randint(1, 100) <= 90:
                new_account()
                print(f"finished {ith} th 'new account' event")
            else:
                update_account()
                print(f"finished {ith} th 'update account' event")

    return ith + 2

if __name__ == "__main__":
    # delete_all()
    # TPS ~= 10
    with DateTimeTimer() as timer:
        n = run_data_faker()

    # with orm.Session(engine) as session:
    #     for transaction in session.query(Transaction):
    #         print([transaction.note])

    # print(f"n event: {n}")
    # print(f"elapsed: {timer.elapsed}")
    # print(f"TPS: {int(n / timer.elapsed)}")
