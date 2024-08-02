import os

import asyncclick as click
from asyncclick import Group

cli = Group("db")


@cli.command(
    "import",
    help="Load CSV(s) from `file-or-dir` to `table` (schema-qualified table) at `db-url`",
)
@click.argument("file-or-dir", type=click.Path(exists=True, dir_okay=True))
@click.argument("table", type=str)
@click.argument("db-url", type=str)
@click.option(
    "--batch-size",
    "-b",
    type=int,
    default=10_000,
    help="Number of rows to load at once.",
)
@click.option(
    "--file-workers",
    "-w",
    type=int,
    default=3,
    help="Number of workers to use per file.",
)
@click.option(
    "--parallel-files",
    "-p",
    type=int,
    help="Number of files to process simultaneously (only applicable if 'file-or-dir' is a directory).",
)
@click.option(
    "--subdirs",
    "-s",
    is_flag=True,
    type=bool,
    help="Search for CSV files in subdirectories. Only applicable if `file-or-dir` is a directory.",
)
def import_csvs(
    file_or_dir, table, db_url, batch_size, file_workers, parallel_files, subdirs
):
    from .files import import_csvs as _import_csvs

    _import_csvs(
        files=file_or_dir,
        schema_table=table,
        db_url=db_url,
        batch_size=batch_size,
        file_workers=file_workers,
        parallel_files=parallel_files,
        subdirs=subdirs,
    )


@cli.group(help="Export data from a database.")
def export():
    pass


@export.command(
    "append",
    help="Export data from `table` (schema-qualified table) at `db-url`. Save to `save-locs` (Bucket(s) (format: s3://{bucket name}) and/or local director(y|ies) to save files to.).",
)
@click.argument("table", type=str)
@click.argument("db-url", type=str)
@click.argument(
    "save_locs",
    type=str,
    nargs=-1,
)
@click.option(
    "--slice-column",
    "-s",
    type=str,
    default=None,
    help="A column with some kind of sequential ordering (e.g. time) that can be used sort rows within the table or a partition.",
)
@click.option(
    "--partition-column",
    "-p",
    type=str,
    default=None,
    help="A column used to partition table data (e.g. a categorical value).",
)
@click.option(
    "--file-max-size",
    "-sz",
    type=str,
    default=None,
    help="Desired size of export files. Must have suffix 'bytes', 'kb', 'mb', 'gb', 'tb'. (e.g. '500mb'). If None, no limit will be placed on file size.",
)
@click.option(
    "--file-stem-prefix",
    "-pfx",
    type=str,
    default=None,
    help="A prefix put on every export file name.",
)
@click.option(
    "--n-workers",
    "-w",
    type=int,
    default=None,
    help="Number of export tasks to run simultaneously.",
)
def _export_append(
    table,
    db_url,
    save_locs,
    slice_column,
    partition_column,
    file_max_size,
    file_stem_prefix,
    n_workers,
):
    from .export.append import export_append

    export_append(
        table=table,
        engine=db_url,
        save_locs=save_locs,
        slice_column=slice_column,
        partition_column=partition_column,
        file_max_size=file_max_size,
        file_stem_prefix=file_stem_prefix,
        n_workers=n_workers,
    )


@export.command(
    "query", help="Export the results of a `query` at `db-url` to `save-path`."
)
@click.argument("query", type=str)
@click.argument("db-url", type=str)
@click.argument("save-path", type=str)
@click.option("--append", "-a", is_flag=True, help="Append to file if already exists.")
def _export_query(query, db_url, save_path, append):
    from .export.utils import psql_copy_to_csv

    psql_copy_to_csv(
        to_copy=query,
        save_path=save_path,
        engine=db_url,
        append=append,
    )


@export.command(
    "hypertable-chunks",
    help="Export all chunks of a TimescaleDB hypertable (`table`) at `db-url`. One file per chunk.",
)
@click.argument(
    "table",
    type=str,
)
@click.argument("db-url", type=str)
@click.argument(
    "save_locs",
    type=str,
    nargs=-1,
    # help="Bucket(s) (format: s3://{bucket name}) and/or local director(y|ies) to save files to.",
)
@click.option(
    "--n-workers",
    type=int,
    default=None,
    help="Number of export tasks to run simultaneously.",
)
def _export_hypertable_chunks(
    table,
    db_url,
    save_locs,
    n_workers,
):
    from .export.append import export_hypertable_chunks

    export_hypertable_chunks(
        engine=db_url,
        table=table,
        save_locs=save_locs,
        n_workers=n_workers,
    )


@cli.command(
    "transfer",
    help="Copy data from one table to another. Tables may be in different databases or on different servers.",
)
@click.argument("src", type=str)
@click.argument("dst_table", type=str)
@click.argument("src_engine", type=str)
@click.argument("dst_engine", type=str)
@click.option(
    "--insert", "-i", is_flag=True, help="Use an insert statement instead of upsert."
)
@click.option("--transaction-rows", "-r", type=int, default=1000)
@click.option(
    "--no-stream",
    "-ns",
    is_flag=True,
    help="Read all data at once instead of streaming.",
)
@click.option("--slice-column", "-s", type=str, default=None)
@click.option("--partition-column", "-p", type=str, default=None)
@click.option("--task_max_size", "-sz", type=str, default="5gb")
@click.option("--n-workers", "-n", type=int, default=4)
@click.option("--finished-tasks-file", "-f", type=str, default=None)
def _transfer(
    src,
    dst_table,
    src_engine,
    dst_engine,
    insert,
    transaction_rows,
    no_stream,
    slice_column,
    partition_column,
    task_max_size,
    n_workers,
    finished_tasks_file,
):
    from .transfer import copy_table_data

    copy_table_data(
        src=src,
        dst_table=dst_table,
        src_engine=src_engine,
        dst_engine=dst_engine,
        upsert=not insert,
        transaction_rows=transaction_rows,
        stream=not no_stream,
        slice_column=slice_column,
        partition_column=partition_column,
        task_max_size=task_max_size,
        n_workers=n_workers,
        finished_tasks_file=finished_tasks_file,
    )


@cli.command()
@click.argument("search-in")
@click.option(
    "--pg-url",
    "-c",
    default=os.environ.get("POSTGRES_URL", ""),
    help="URL of PostreSQL database to create tables in.",
)
@click.option(
    "--recreate", "-r", default=False, help="Recreate tables that currently exist."
)
async def create_tabless(search_in: str, pg_url: str, recreate: bool):
    """Create all tables found in provided module or package path."""
    from sqlalchemy.ext.asyncio import create_async_engine

    from dbflows import async_create_tables
    from dbflows.utils import driver_pg_url

    engine = create_async_engine(driver_pg_url("asyncpg", pg_url))
    click.echo(f"Searching for tables in {search_in}")
    await async_create_tables(
        engine=engine,
        create_from=search_in,
        recreate=recreate,
    )


def run_cli():
    cli(_anyio_backend="asyncio")


if __name__ == "__main__":
    run_cli()
