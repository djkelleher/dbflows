import re
from collections import defaultdict
from pprint import pformat
from types import ModuleType
from typing import List, Optional, Sequence, Union

import duckdb
import sqlalchemy as sa
from fileflows.s3 import S3, S3Cfg
from sqlalchemy.orm.decl_api import DeclarativeMeta
from tqdm import tqdm

from dbflows import cached_sa_conn

from .tables import create_tables
from .utils import logger, schema_table


async def create_tables_from_sources(
    table_sources: Sequence[Union[sa.Table, DeclarativeMeta, ModuleType]],
    pg_url: str,
    recreate_tables: bool = False,
) -> List[sa.Table]:
    """
    Create tables in the database from the given sources.

    Args:
        table_sources: A sequence of SQLAlchemy :class:`Table` objects, or
            declarative class definitions, or modules containing them.
        pg_url: PostgreSQL connection URL.
        recreate_tables: If True, drop and recreate tables if they already exist.

    Returns:
        List of created SQLAlchemy :class:`Table` objects.
    """
    # Make sure table_sources is a sequence.
    if not isinstance(table_sources, (list, tuple)):
        table_sources = [table_sources]
    # find table definition objects.
    created_tables = []
    # Get a connection to the database.
    pg = cached_sa_conn(pg_url)
    # Start a transaction
    async with pg.begin() as conn:
        # Iterate over the table sources.
        for src in table_sources:
            # Create the tables.
            created_tables += await create_tables(
                conn=conn, create_from=src, recreate=recreate_tables
            )
    # Done.
    return created_tables


async def initialize_db(
    pg_url: str,
    table_sources: Sequence[Union[sa.Table, DeclarativeMeta, ModuleType]],
    recreate_tables: bool = False,
    files_bucket: Optional[str] = None,
    files_partition: Optional[str] = None,
    load_files_to_existing_tables: bool = False,
    s3_cfg: Optional[S3Cfg] = None,
):
    """
    Initialize a PostgreSQL database by creating tables from the given sources
    and loading files from the given S3 bucket and partition.

    Args:
        pg_url: PostgreSQL connection URL.
        table_sources: Sequence of SQLAlchemy :class:`Table` objects, or
            declarative class definitions, or modules containing them.
        recreate_tables: If True, drop and recreate tables if they already exist.
        files_bucket: S3 bucket name. If None, no files are loaded.
        files_partition: S3 partition name. If None, all files in the bucket are loaded.
        load_files_to_existing_tables: If True, load files to existing tables.
            If False, only load files to tables that were created in this call.
        s3_cfg: S3 config object. If None, use default AWS config.
    """
    # create tables from given sources (Table objects, declarative class definitions,
    # or modules containing them).
    created_tables = await create_tables_from_sources(
        table_sources=table_sources, recreate_tables=recreate_tables
    )
    # if no files are given, bail.
    if files_bucket is None:
        return
    # otherwise, find files with data for tables.
    s3 = S3(s3_cfg=s3_cfg)
    files = s3.list_files(
        bucket_name=files_bucket,
        partition=files_partition,
        return_as="urls"
    )
    logger.info(
        "Found %i files in %s %s", len(files), files_bucket, files_partition or ""
    )
    # map schame.table to file URL.
    table_file_urls = defaultdict(list)
    for f in files:
        # for each file, find table name from file URL. Format is:
        # <...>T(some_schema.some_table)
        if m := re.search(r"T\(([^)]+)\)", f):
            table_file_urls[m.group(1)].append(f)
        else:
            logger.warning("Could not parse table name from %s", f)
    # if we only want to load files to tables that were created in this call,
    # filter the table_file_urls dictionary to only include those tables.
    if not load_files_to_existing_tables:
        table_names = [schema_table(table) for table in created_tables]
        table_file_urls = {name: table_file_urls.get(name) for name in table_names}
    # if there are no files to load, bail.
    if not table_file_urls:
        logger.info("No files to load to database.")
        return
    # attach the PostgreSQL database to DuckDB.
    pg_db_name = pg_url.split("/")[-1]
    duckdb.execute(f"ATTACH '{pg_url}' AS {pg_db_name} (TYPE POSTGRES)")
    # for each table, load all files associated with it.
    for table_name, table_files in table_file_urls.items():
        logger.info(
            "Loading %i files to %s:\n%s",
            len(table_files),
            table_name,
            pformat(table_files),
        )
        for file in tqdm(table_files):
            # use DuckDB to copy the file to the table.
            duckdb.execute(f"COPY {pg_db_name}.{table_name} FROM '{file}';")
