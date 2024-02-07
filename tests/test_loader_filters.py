from uuid import uuid4

import pytest
from dbgress.load import Loader


@pytest.fixture
def db_loader_creator(async_engine, single_column_table):
    async def _db_loader_creator(**kwargs):
        loader = await Loader.create(
            table=single_column_table, engine=async_engine, **kwargs
        )
        return loader

    return _db_loader_creator


@pytest.mark.asyncio
async def test_remove_duplicate_key_rows(
    single_column_table, random_str_rows, async_engine, db_loader_creator
):
    rows = random_str_rows(single_column_table, 5)
    n_dupes = 2
    rows += rows[:n_dupes]
    loader = await db_loader_creator(remove_duplicate_key_rows=True)
    filtered_rows = loader.filter_rows(rows)
    assert len(filtered_rows) == len(rows) - n_dupes


@pytest.mark.asyncio
async def test_remove_rows_missing_key(db_loader_creator):
    loader = await db_loader_creator(remove_rows_missing_key=True)
    rows = [{str(uuid4()): str(uuid4())}]
    rows = loader.filter_rows(rows)
    assert not len(rows)


@pytest.mark.asyncio
async def test_column_name_map(db_loader_creator):
    column_name_map_from = str(uuid4())
    loader = await db_loader_creator(column_name_map={column_name_map_from: "column"})
    rows = [{column_name_map_from: str(uuid4())}]
    rows = loader.filter_rows(rows)
    assert len(rows) == 1
    assert "column" in rows[0]


@pytest.mark.asyncio
async def test_all_names_converter(db_loader_creator):
    loader = await db_loader_creator(column_names_converter=lambda v: f"{v}umn")
    rows = [{"col": str(uuid4())}]
    rows = loader.filter_rows(rows)
    assert len(rows) == 1
    assert "column" in rows[0]


@pytest.mark.asyncio
async def test_column_name_converters(db_loader_creator):
    loader = await db_loader_creator(
        column_name_converters={"col": lambda v: f"{v}umn"},
    )
    rows = [{"col": str(uuid4())}]
    rows = loader.filter_rows(rows)
    assert len(rows) == 1
    assert "column" in rows[0]


@pytest.mark.asyncio
async def test_value_map(db_loader_creator):
    loader = await db_loader_creator(value_map={"map_from": "map_to"})
    rows = [{"column": "map_from"}]
    rows = loader.filter_rows(rows)
    assert len(rows) == 1
    assert rows[0]["column"] == "map_to"


@pytest.mark.asyncio
async def test_all_values_converter(db_loader_creator):
    loader = await db_loader_creator(column_values_converter=lambda v: v + v)
    rows = [{"column": "value"}]
    rows = loader.filter_rows(rows)
    assert len(rows) == 1
    assert rows[0]["column"] == "valuevalue"


@pytest.mark.asyncio
async def test_column_value_converters(db_loader_creator):
    loader = await db_loader_creator(
        column_value_converters={"column": lambda v: v + v}
    )
    rows = [{"column": "value"}]
    rows = loader.filter_rows(rows)
    assert len(rows) == 1
    assert rows[0]["column"] == "valuevalue"


@pytest.mark.asyncio
async def test_drop_non_table_columns(db_loader_creator):
    loader = await db_loader_creator()
    column = str(uuid4())
    rows = [{column: str(uuid4())}]
    rows = loader.filter_rows(rows)
    assert len(rows) == 0
