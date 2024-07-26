import re
from enum import EnumMeta
from pprint import pformat
from types import ModuleType
from typing import List, Optional, Sequence, Union

import sqlalchemy as sa
from dynamic_imports import class_inst
from sqlalchemy import func as fn
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.schema import CreateTable

from .utils import compile_statement, logger, schema_table, to_table

tables_table = sa.Table(
    "tables",
    sa.MetaData(schema="information_schema"),
    sa.Column("table_schema", sa.Text),
    sa.Column("table_name", sa.Text),
)

metric_abbrs = {
    "average": "avg",
    "kurtosis": "kurt",
    "stddev": "std",
}


def format_table_name(name: str) -> str:
    table = re.sub(r"\s+", "_", name).lower()
    table = re.sub(r"^[0-9]", lambda m: "_" + m.group(), table)
    for metric, abbr in metric_abbrs.items():
        table = table.replace(metric, abbr)
    return table


# TODO HypterTable
# SELECT add_compression_policy('table name', INTERVAL '7 days');
# ALTER TABLE td_ameritrade.option_payloads_tz SET (timescaledb.compress, timescaledb.compress_orderby = 'updated DESC');
# https://docs.postgres.com/timescaledb/latest/how-to-guides/compression/about-compression/
# https://docs.timescale.com/api/latest/compression/


def drop_table_statement(schema_table: str, cascade: bool = False) -> str:
    statement = f"DROP TABLE {schema_table}"
    if cascade:
        statement += " CASCADE"
    return statement


def list_tables_statement(
    schema: Optional[str] = None, like_pattern: Optional[str] = None
) -> str:
    """Query all tables in the database. Returns scalars."""
    query = sa.select(
        fn.concat(tables_table.c.table_schema, ".", tables_table.c.table_name).label(
            "table"
        )
    )
    if schema:
        query = query.where(tables_table.c.table_schema == schema)
    if like_pattern:
        query = query.where(tables_table.c.table_name.like(like_pattern))
    return compile_statement(query)


def table_exists_query(table: sa.Table) -> str:
    """Check if a table exists in the database. Returns scalar."""
    return compile_statement(
        sa.select(
            sa.exists(sa.text("1"))
            .select_from(tables_table)
            .where(
                tables_table.c.table_schema == table.schema,
                tables_table.c.table_name == table.name,
            )
        ),
    )


async def create_tables(
    conn,
    create_from: Union[sa.Table, DeclarativeMeta, ModuleType, Sequence],
    recreate: bool = False,
) -> List[sa.Table]:
    """Create a table in the database.

    Args:
        conn: Connection to use for executing SQL statements.
        create_from (Union[sa.Table, Sequence[sa.Table], ModuleType]): The entity or table object to create a database table for.
        recreate (bool): If table exists, drop it and recreate. Defaults to False.

    Returns:
        List[Union[sa.Table, DeclarativeMeta]]: The created tables.
    """
    if isinstance(create_from, (sa.Table, DeclarativeMeta)):
        tables = [create_from]
    elif not isinstance(create_from, (list, tuple)):
        tables = class_inst(class_type=sa.Table, search_in=create_from)
        tables += class_inst(class_type=DeclarativeMeta, search_in=create_from)
    else:
        tables = create_from
    logger.info(
        "Checking %i tables existence: %s",
        len(tables),
        pformat([schema_table(t) for t in tables]),
    )
    created_tables = []
    for table in tables:
        table = to_table(table)
        exists = await conn.fetchval(table_exists_query(table))
        if exists:
            continue
        # create referenced tables first.
        for col in table.columns.values():
            for fk in col.foreign_keys:
                fk_tbl = fk.column.table
                logger.info("Checking foreign key table %s", schema_table(fk_tbl))
                await create_tables(conn, fk_tbl, recreate)
        if schema := table.schema:
            # create schema if needed.
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        else:
            schema = "public"
        table_name = table.name
        if recreate:
            await conn.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}" CASCADE')
        # create enum types if they do not already exist in the database.
        # this is necessary because sa.Table.create has no way to handle columns that have an enum type that already exists in the Database.
        enums = [
            col.type
            for col in table.columns.values()
            if isinstance(col.type, (postgresql.ENUM, EnumMeta))
        ]
        # TODO create enums.
        # for enum in enums:
        # check if enum type exists, create it if it doesn't.
        # enum.create(conn, checkfirst=True)
        # prevent table.create for attempting to create the enum type.
        # enum.create_type = False

        statement = CreateTable(table, if_not_exists=True).compile(
            dialect=postgresql.dialect()
        )
        await conn.execute(str(statement))
        # create hypertable if needed.
        await create_hypertable(conn, table)
        created_tables.append(table)
    return created_tables


async def create_hypertable(conn, table: Union[sa.Table, DeclarativeMeta]) -> None:
    # TODO secondary partition as comment pattern.
    """Create a hypertable partition if table or entity has a column that is flagged as a hypertable partition column.
    For numeric columns, hypertable partitions should be specify the unit of the values in the column (seconds, miliseconds, microseconds, nanoseconds),
    e.g. `hypertable-partition (miliseconds, 14 days)`.
    For datetime columns, only the number of days needs to be specified: e.g. `hypertable-partition (14 days)`

    Args:
        table (Union[sa.Table, DeclarativeMeta]): The entity or table that should be checked.
    """
    table = to_table(table)
    # find hypertable time column.
    second_scalars = {
        "seconds": 10**0,
        "miliseconds": 10**3,
        "microseconds": 10**6,
        "nanoseconds": 10**9,
    }
    time_column, chunk_time_interval = None, None
    for col_name, col in table.columns.items():
        if col.comment and (
            partition_cfg := re.search(
                r"(?i)hypertable[\s_-]partition\s?\((?:((?:mili|micro|nano)?seconds),)?\s?(\d{1,4})\s?(?:d|days?)\)",
                col.comment,
            )
        ):
            if time_column is not None:
                raise ValueError(
                    "Multiple hypertable time columns found. Can not resolve configuration."
                )
            time_column = col_name
            chunk_days = partition_cfg.group(2)
            if int_time_unit := partition_cfg.group(1):
                chunk_time_interval = (
                    86_400 * second_scalars[int_time_unit] * int(chunk_days)
                )
            else:
                chunk_time_interval = f"INTERVAL '{chunk_days} days'"

    if time_column:
        schema = table.schema or "public"
        statement = f"SELECT create_hypertable('{schema}.{table.name}', '{time_column}', chunk_time_interval => {chunk_time_interval})"
        logger.info(
            "Creating hypertable partition: %s.",
            statement,
        )
        await conn.execute(statement)
