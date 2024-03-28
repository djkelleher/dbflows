from copy import deepcopy
from random import randint
from uuid import uuid4

import asyncpg
import pytest
import sqlalchemy as sa

from dbflows.load import PgLoader, groupby_columns


def row_count_query(single_column_table):
    return sa.select(sa.func.count()).select_from(single_column_table)


@pytest.mark.asyncio
async def test_rows_load(key_value_table, random_str_rows, engine, temp_db):
    """Check that rows load to database."""
    loader = await PgLoader.create(key_value_table, pg_url=temp_db)
    n_rows = randint(5, 20)
    await loader.load_rows(random_str_rows(key_value_table, n_rows))
    with engine.begin() as conn:
        res = conn.execute(row_count_query(key_value_table))
        db_row_count = res.scalar()
    assert db_row_count == n_rows


@pytest.mark.asyncio
async def test_batches_load(key_value_table, random_str_rows, engine, temp_db):
    """Check that rows load to database."""

    loader = await PgLoader.create(key_value_table, row_batch_size=5, pg_url=temp_db)
    n_rows = randint(10, 20)
    await loader.load_rows(random_str_rows(key_value_table, n_rows))
    with engine.begin() as conn:
        res = conn.execute(row_count_query(key_value_table))
        db_row_count = res.scalar()
    assert db_row_count == n_rows


@pytest.mark.asyncio
async def test_on_dupe_ignore(key_value_table, random_str_rows, engine, temp_db):
    loader = await PgLoader.create(
        key_value_table, on_duplicate_key_update=False, pg_url=temp_db
    )
    n_rows = randint(10, 20)
    rows = random_str_rows(key_value_table, n_rows)

    # load rows.
    await loader.load_rows(rows)

    n_pkey_to_update = 5
    for row in rows[:n_pkey_to_update]:
        # rows with new key should get inserted.
        row["key"] += str(uuid4())
    for row in rows[n_pkey_to_update:]:
        # rows with same key should be ignore regardless of updated data.
        row["data"] += str(uuid4())
    # check that duplicate rows get ignored without error and new rows get inserted.
    await loader.load_rows(rows)
    with engine.begin() as conn:
        res = conn.execute(row_count_query(key_value_table))
        db_row_count = res.scalar()
    assert db_row_count == n_rows + n_pkey_to_update
    local_values_updated = {r["data"] for r in rows[n_pkey_to_update:]}
    local_values_updated_keys = [r["key"] for r in rows[n_pkey_to_update:]]
    # check that duplicate key rows with values that were updated locally do not have their values updated in the database.
    query = sa.select(key_value_table.c.data).where(
        key_value_table.c.key.in_(local_values_updated_keys)
    )
    with engine.begin() as conn:
        res = conn.execute(query)
        db_values = set(res.scalars())

    assert not len(db_values.intersection(local_values_updated))


@pytest.mark.asyncio
async def test_on_duplicate_key_update_column(
    key_multi_value_table, random_str_rows, engine, temp_db
):
    loader = await PgLoader.create(
        key_multi_value_table, on_duplicate_key_update=["data1"], pg_url=temp_db
    )

    n_rows = randint(5, 20)
    rows = random_str_rows(key_multi_value_table, n_rows)

    # load original rows.
    await loader.load_rows(rows)

    # update row values.
    updated_rows = deepcopy(rows)
    for row in updated_rows:
        row["data1"] += str(uuid4())
        row["data2"] += str(uuid4())

    # load updated rows.
    await loader.load_rows(updated_rows)

    # check that all rows in database have data1 value updated.
    with engine.begin() as conn:
        res = conn.execute(sa.select(key_multi_value_table.c.data1))
        db_data1 = set(res.scalars())
    assert not len(db_data1.intersection([r["data1"] for r in rows]))

    # check that no rows in database have data2 value updated.
    with engine.begin() as conn:
        res = conn.execute(sa.select(key_multi_value_table.c.data2))
        db_data2 = set(res.scalars())
    assert not len(db_data2.symmetric_difference([r["data2"] for r in rows]))


@pytest.mark.asyncio
async def test_on_duplicate_key_update(
    key_multi_value_table, random_str_rows, temp_db, engine
):
    loader = await PgLoader.create(
        key_multi_value_table, on_duplicate_key_update=True, pg_url=temp_db
    )

    n_rows = randint(5, 20)
    rows = random_str_rows(key_multi_value_table, n_rows)

    # load original rows.
    await loader.load_rows(rows)

    # update row values.
    updated_rows = deepcopy(rows)
    for row in updated_rows:
        row["data1"] += str(uuid4())
        row["data2"] += str(uuid4())

    # load updated rows.
    await loader.load_rows(updated_rows)

    # check that all rows in database have data1 value updated.
    with engine.begin() as conn:
        res = conn.execute(sa.select(key_multi_value_table.c.data1))
        db_data1 = set(res.scalars())
    assert not len(db_data1.intersection([r["data1"] for r in rows]))

    # check that all rows in database have data2 value updated.
    with engine.begin() as conn:
        res = conn.execute(sa.select(key_multi_value_table.c.data2))
        db_data2 = set(res.scalars())
    assert not len(db_data2.intersection([r["data2"] for r in rows]))


def test_group_by_columns():
    rows = [
        {"key": str(uuid4()), "data1": str(uuid4()), "data2": str(uuid4())},
        {"key": str(uuid4()), "data1": str(uuid4())},
    ]
    groups = list(groupby_columns(rows))
    assert len(groups) == 2
    assert isinstance(groups[0], list)
    assert isinstance(groups[1], list)


@pytest.mark.asyncio
async def test_fetch(key_multi_value_table, temp_db):
    loader = await PgLoader.create(key_multi_value_table, pg_url=temp_db)
    key1 = str(uuid4())
    rows = [
        {"key": key1, "data1": str(uuid4()), "data2": str(uuid4())},
        {"key": str(uuid4()), "data1": str(uuid4()), "data2": str(uuid4())},
    ]
    await loader.load_rows(rows)
    fetched_rows = await loader.fetch(sa.select(key_multi_value_table))
    assert len(fetched_rows) == len(rows)


@pytest.mark.asyncio
async def test_fetchrow(key_multi_value_table, temp_db):
    loader = await PgLoader.create(key_multi_value_table, pg_url=temp_db)
    key1 = str(uuid4())
    rows = [
        {"key": key1, "data1": str(uuid4()), "data2": str(uuid4())},
        {"key": str(uuid4()), "data1": str(uuid4()), "data2": str(uuid4())},
    ]
    await loader.load_rows(rows)
    fetched_row = await loader.fetchrow(
        sa.select(key_multi_value_table).where(key_multi_value_table.c.key == key1)
    )
    assert isinstance(fetched_row, asyncpg.Record)


@pytest.mark.asyncio
async def test_fetchval(key_multi_value_table, temp_db):
    loader = await PgLoader.create(key_multi_value_table, pg_url=temp_db)
    key1 = str(uuid4())
    rows = [
        {"key": key1, "data1": str(uuid4()), "data2": str(uuid4())},
        {"key": str(uuid4()), "data1": str(uuid4()), "data2": str(uuid4())},
    ]
    await loader.load_rows(rows)
    fetched_value = await loader.fetchval(
        sa.select(key_multi_value_table.c.data1).where(
            key_multi_value_table.c.key == key1
        )
    )
    assert isinstance(fetched_value, str)
