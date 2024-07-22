import re
from collections import defaultdict
from types import ModuleType
from typing import Optional, Sequence, Union

import duckdb
import sqlalchemy as sa
from fileflows.s3 import S3, S3Cfg
from sqlalchemy.orm.decl_api import DeclarativeMeta

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
    # map schame.table to file URL.
    table_file_urls = defaultdict(list)
    for f in files:
        table_file_urls[re.search(r"T\(([^)]+)\)", f).group(1)].append(f)
    if not load_files_to_existing_tables:
        table_names = [schema_table(table) for table in created_tables]
        table_file_urls = {name: table_file_urls.get(name) for name in table_names}
    if not table_file_urls:
        logger.info("No files to load to database.")
        return
    duckdb.execute(f"ATTACH '{pg_url}' AS pgdb (TYPE POSTGRES)")
    for table_name, table_files in table_file_urls.items():
        table_files = ",".join([f"'{f}'" for f in sorted(table_files)])
        logger.info("Loading %i files to %s", len(table_files), table_name)
        # f"SELECT * FROM read_csv([{table_files}], union_by_name = true);"
