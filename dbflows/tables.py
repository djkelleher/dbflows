import re
from enum import EnumMeta
from pprint import pformat
from types import ModuleType
from typing import List, Optional, Sequence, Union

import asyncpg
import sqlalchemy as sa
from dynamic_imports import class_inst
from sqlalchemy import func as fn
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.schema import CreateTable

from .conn import PgConn
from .utils import logger, schema_table, to_table

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

async def drop_table(conn: PgConn, table: str | sa.Table, cascade: bool = True):
    if not isinstance(table,str):
        table = schema_table(table)
    logger.info("Dropping table %s. Cascade: %s", table, cascade)
    statement = f"DROP TABLE {table}"
    if cascade:
        statement += " CASCADE"
    return await conn.execute(sa.text(statement))


async def drop_tables(
    conn: PgConn, schema: Optional[str] = None, like_pattern: Optional[str] = None
) -> List[str]:
    tables = await list_tables(conn, schema, like_pattern)
    for table in tables:
        await drop_table(conn, table)
    

async def list_tables(
    conn: PgConn, schema: Optional[str] = None, like_pattern: Optional[str] = None
) -> List[str]:
    """Query all tables in the database. Returns scalars."""
    query = sa.select(
        fn.concat('"', tables_table.c.table_schema,'"', ".",'"', tables_table.c.table_name,'"').label(
            "table"
        )
    )
    if schema:
        query = query.where(tables_table.c.table_schema == schema)
    if like_pattern:
        query = query.where(tables_table.c.table_name.like(like_pattern))
    return await conn.scalars(query)



def table_exists_query(table: sa.Table):
    return sa.select(
        sa.exists(sa.text("1"))
        .select_from(tables_table)
        .where(
            tables_table.c.table_schema == table.schema,
            tables_table.c.table_name == table.name,
        )
    )


async def table_exists(conn: PgConn, table: sa.Table) -> bool:
    """Check if a table exists in the database. Returns scalar."""
    return await conn.fetchval(table_exists_query(table))


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
        exists = await conn.execute(table_exists_query(table))
        exists = exists.scalar()
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
            try:
                await conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            except asyncpg.exceptions.UniqueViolationError:
                # possible error from coroutine execution.
                pass
        else:
            schema = "public"
        table_name = table.name
        if recreate:
            await conn.execute(sa.text(f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE'))
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
        await conn.execute(sa.text(str(statement)))
        # create hypertable if needed.
        await create_hypertable(conn, table)
        created_tables.append(table)
    return created_tables


async def create_hypertable(conn, table: Union[sa.Table, DeclarativeMeta]) -> None:
    """
    Create a TimescaleDB hypertable partition if table or entity has a column that is flagged as a hypertable partition column.

    For numeric columns, hypertable partitions should be specify the unit of the values in the column (seconds, miliseconds, microseconds, nanoseconds),
    e.g. comment="hypertable-partition (1 day), compress_after_days 1, drop_after_days 3 "
    For datetime columns, only the number of days needs to be specified: e.g. `hypertable-partition (14 days)`

    Args:
        table (Union[sa.Table, DeclarativeMeta]): The entity or table that should be checked.
    """
    table = to_table(table)
    table_name  = schema_table(table) 
    # find hypertable time column.
    second_scalars = {
        "seconds": 10**0,
        "miliseconds": 10**3,
        "microseconds": 10**6,
        "nanoseconds": 10**9,
    }
    # this variable will hold the name of the time column.
    time_column = None
    # loop over all columns of the table.
    for col_name, col in table.columns.items():
        # if the column has a comment, check if it matches the regular expression for a hypertable time column.
        if col.comment and (
            partition_cfg := re.search(
                r"(?i)hypertable[\s_-]partition\s?\((?:((?:mili|micro|nano)?seconds),)?\s?(\d{1,4})\s?(?:d|days?)\)",
                col.comment,
            )
        ):
            # if we have already found a time column, raise an error.
            if time_column is not None:
                raise ValueError(
                    "Multiple hypertable time columns found. Can not resolve configuration."
                )
            # remember the name of the time column.
            time_column = col_name
            # get the number of days from the regular expression match.
            chunk_days = partition_cfg.group(2)
            # if the regular expression matched a time unit (miliseconds, microseconds, nanoseconds), convert it to seconds.
            if int_time_unit := partition_cfg.group(1):
                # convert the interval to an integer of seconds.
                chunk_time_interval = (
                    86_400 * second_scalars[int_time_unit] * int(chunk_days)
                )
            else:
                # if the regular expression didn't match a time unit, just use the number of days as an interval.
                chunk_time_interval = f"INTERVAL '{chunk_days} days'"
            if drop_after_days := re.search(r"drop_after_days\s*(\d+)", col.comment):
                drop_after_days = drop_after_days.group(1)
            compress_after_days = re.search(r"compress_after_days\s*(\d+)", col.comment)
            if compress_after_days:
                compress_after_days = compress_after_days.group(1)
    # if a time column was found, create a hypertable partition.
    if time_column and chunk_time_interval:
        statement = f"SELECT create_hypertable('{table_name}', '{time_column}', chunk_time_interval => {chunk_time_interval})"
        logger.info("Creating hypertable partition: %s.",statement)
        await conn.execute(sa.text(statement))
        if drop_after_days:
            logger.info("Dropping hypertable chunks after %s days", drop_after_days)
            await conn.execute(sa.text(f"SELECT drop_chunks('{table_name}', INTERVAL '{drop_after_days} days')"))
        if compress_after_days:
            logger.info("Adding compression policy to compress after %s days.",compress_after_days)
            await conn.execute(sa.text(f"""ALTER TABLE {table_name} SET (
                timescaledb.compress,
                timescaledb.compress_orderby = '{time_column} DESC'
            );"""))
            await conn.execute(sa.text(f"SELECT add_compression_policy('{table_name}', INTERVAL '{compress_after_days} days');"))