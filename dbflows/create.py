import re
from collections import defaultdict
from pprint import pformat
from types import ModuleType
from typing import Optional, Sequence, Union

import duckdb
import sqlalchemy as sa
from fileflows.s3 import S3, S3Cfg
from sqlalchemy.orm.decl_api import DeclarativeMeta
from tqdm import tqdm

from .tables import create_tables
from .utils import get_connection_pool, logger, schema_table


async def initialize_db(
    pg_url: str,
    table_sources: Sequence[Union[sa.Table, DeclarativeMeta, ModuleType]],
    recreate_tables: bool = False,
    files_bucket: Optional[str] = None,
    files_partition: Optional[str] = None,
    load_files_to_existing_tables: bool = False,
    s3_cfg: Optional[S3Cfg] = None,
):
    if not isinstance(table_sources, (list, tuple)):
        table_sources = [table_sources]
    # find table definition objects.
    created_tables = []
    pg = await get_connection_pool(pg_url)
    async with pg.acquire() as conn:
        for src in table_sources:
            created_tables += await create_tables(
                conn=conn, create_from=src, recreate=recreate_tables
            )
    if files_bucket is None:
        return
    # find files with data for tables.
    s3 = S3(s3_cfg=s3_cfg)
    files = s3.list_files(
        bucket_name=files_bucket,
        partition=files_partition,
        return_as="urls",
        pattern=None,
    )
    logger.info(
        "Found %i files in %s %s.", len(files), files_bucket, files_partition or ""
    )
    # map schame.table to file URL.
    table_file_urls = defaultdict(list)
    for f in files:
        if m := re.search(r"T\(([^)]+)\)", f):
            table_file_urls[m.group(1)].append(f)
        else:
            logger.warning("Could not parse table name from %s", f)
    if not load_files_to_existing_tables:
        table_names = [schema_table(table) for table in created_tables]
        table_file_urls = {name: table_file_urls.get(name) for name in table_names}
    if not table_file_urls:
        logger.info("No files to load to database.")
        return
    pg_db_name = "trading"
    duckdb.execute(f"ATTACH '{pg_url}' AS {pg_db_name} (TYPE POSTGRES)")
    for table_name, table_files in table_file_urls.items():
        logger.info(
            "Loading %i files to %s:\n%s",
            len(table_files),
            table_name,
            pformat(table_files),
        )
        for file in tqdm(table_files):
            duckdb.execute(f"COPY {pg_db_name}.{table_name} FROM '{file}';")
