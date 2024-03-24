from copy import deepcopy
from random import randint
from uuid import uuid4
import asyncpg

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import CompileError
from sqlalchemy.dialects import postgresql
from asyncpg.exceptions import CardinalityViolationError

from dbflows.load import PgLoader, groupby_columns
from sqlalchemy.exc import CompileError


def row_count_query(single_column_table):
    return sa.select(sa.func.count()).select_from(single_column_table)


@pytest.mark.asyncio
async def test_rows_load(key_value_table, random_str_rows, engine, temp_db):
    """Check that rows load to database."""
    loader = await PgLoader.create(key_value_table, dsn=temp_db)
    n_rows = randint(5, 20)
    await loader.load_rows(random_str_rows(key_value_table, n_rows))
    with engine.begin() as conn:
        res = conn.execute(row_count_query(key_value_table))
        db_row_count = res.scalar()
    assert db_row_count == n_rows


@pytest.mark.asyncio
async def test_batches_load(key_value_table, random_str_rows, engine, temp_db):
    """Check that rows load to database."""

    loader = await PgLoader.create(key_value_table, row_batch_size=5, dsn=temp_db)
    n_rows = randint(10, 20)
    await loader.load_rows(random_str_rows(key_value_table, n_rows))
    with engine.begin() as conn:
        res = conn.execute(row_count_query(key_value_table))
        db_row_count = res.scalar()
    assert db_row_count == n_rows


@pytest.mark.asyncio
async def test_on_dupe_ignore(key_value_table, random_str_rows, engine, temp_db):
    loader = await PgLoader.create(
        key_value_table, on_duplicate_key_update=False, dsn=temp_db
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
        key_multi_value_table, on_duplicate_key_update=["data1"], dsn=temp_db
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
        key_multi_value_table, on_duplicate_key_update=True, dsn=temp_db
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
async def test_compile_error(key_multi_value_table, temp_db):
    loader = await PgLoader.create(
        key_multi_value_table, group_by_columns_present=False, dsn=temp_db
    )
    rows = [
        {"key": str(uuid4()), "data1": str(uuid4()), "data2": str(uuid4())},
        {"key": str(uuid4()), "data1": str(uuid4())},
    ]
    with pytest.raises(
        CompileError,
        match="is explicitly rendered as a boundparameter in the VALUES clause",
    ):
        loader._build_statement(rows).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})


@pytest.mark.asyncio
async def test_cardinality_violation_error(key_value_table, temp_db):
    loader = await PgLoader.create(
        key_value_table, duplicate_key_rows_keep="first", dsn=temp_db
    )
    key = str(uuid4())
    first_value = str(uuid4())
    rows = [
        {"key": key, "data": first_value},
        {"key": key, "data": str(uuid4())},
    ]
    statement = loader._build_statement(rows).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    conn = await asyncpg.connect(temp_db)
    with pytest.raises(
        CardinalityViolationError,
        match="command cannot affect row a second time",
    ):
        await conn.execute(str(statement))
    await conn.close()



    