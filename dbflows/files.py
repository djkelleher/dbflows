import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from subprocess import run
from typing import Sequence, Union

from .utils import logger, parse_pg_url, split_schema_table


def import_csvs(
    files: Union[Path, Sequence[Path]],
    schema_table: str,
    db_url: str,
    batch_size: int = 10_000,
    file_workers: int = 3,
    parallel_files: int = None,
    subdirs: bool = False,
    no_run: bool = False,
):
    """Load CSV file(s) into a PostgreSQL/TimescaleDB table using timescaledb-parallel-copy.

    Args:
        files (Union[Path, Sequence[Path]]): Path to file, directory with files, or list of files.
        schema_table (str): Schema-qualified table name. i.e. {schema}.{table}
        db_url (str): URL to database.
        batch_size (int, optional): How many rows to load at once. Defaults to 10_000.
        file_workers (int, optional): How many worker threads to use per file. Defaults to 3.
        parallel_files (int, optional): How many files to process simultaneously. Defaults to None.
        subdirs (bool, optional): Search for CSV files in subdirectories. Only applicable if `file` is a directory. Defaults to False.
        no_run (bool, optional): Don't run the commands. Return command strings. Defaults to False.
    """

    if isinstance(files, Path):
        files = [files]
    elif files.is_dir():
        if subdirs:
            files = list(files.rglob("*.csv"))
        else:
            files = list(files.glob("*.csv"))
    url_parsed = parse_pg_url(db_url)
    # username, password, host, port
    conn_meta = url_parsed.hosts()
    conn_meta["user"] = conn_meta.pop("username")
    conn_meta["sslmode"] = "disable"
    conn_meta = " ".join([f"{k}={v}" for k, v in conn_meta.items()])
    schema, table = split_schema_table(schema_table)
    commands = []
    for file in files:
        kwargs = {
            "connection": f'"{conn_meta}"',
            "db-name": url_parsed.path.lstrip("/"),
            "schema": schema,
            "table": table,
            "file": file,
            "workers": file_workers,
            "reporting-period": "20s",
            "batch-size": batch_size,
        }
        kwargs = " ".join([f"--{k} {v}" for k, v in kwargs.items()])
        # TODO docs for install
        commands.append(f"timescaledb-parallel-copy {kwargs} --verbose")
    if no_run:
        return commands

    def run_command(command: str):
        result = run(command, capture_output=True, shell=True)
        msg = f"({result.returncode}) Finished import {command}."
        if info := result.stdout.decode("utf-8").strip():
            msg += f" Info: {info}."
        if err := result.stderr.decode("utf-8").strip():
            msg += f" Error: {err}."
            logger.error(msg)
        else:
            logger.info(msg)

    if len(commands) == 1:
        return run_command(commands[0])
    n_imports = len(files)
    max_workers = min(
        parallel_files or int(os.cpu_count() * 0.8 / file_workers), n_imports
    )
    logger.info("Importing %i files with %i workers: %s", n_imports, max_workers, files)
    with ThreadPoolExecutor(max_workers=max_workers) as p:
        p.map(run_command, commands)
