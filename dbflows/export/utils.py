import re
from pathlib import Path
from subprocess import run
from typing import Optional, Union

import duckdb
import sqlalchemy as sa
from fileflows import S3Cfg, create_duckdb_secret, is_s3_path
from sqlalchemy.engine import Engine
from xxhash import xxh32

from dbflows.utils import (
    compile_statement,
    engine_url,
    logger,
    remove_engine_driver,
    schema_table,
)


def psql_copy_to_csv(
    to_copy: Union[str, sa.Table, sa.select],
    save_path: Union[Path, str],
    engine: Union[str, Engine],
    append: bool,
):
    """Copy a table or query result to a csv file."""
    to_copy = (
        schema_table(to_copy)
        if isinstance(to_copy, sa.Table)
        else f"({compile_statement(to_copy)})"
    )
    save_path = Path(save_path)
    if not save_path.exists() and save_path.suffix != ".gz":
        copy_to = f"'{save_path}'"
    else:
        program = "gzip" if save_path.suffix == ".gz" else "cat"
        operator = ">>" if append else ">"
        copy_to = f"""PROGRAM '{program} {operator} "{save_path}"'"""
    save_path.parent.mkdir(parents=True, exist_ok=True)
    db_url = remove_engine_driver(engine_url(engine))
    psql_code = " ".join(
        [r"\copy", to_copy, "TO", copy_to, "DELIMITER ',' CSV"]
    ).replace('"', '\\"')
    cmd = f"""psql "{db_url}" -c "{psql_code}\""""
    # cmd = f"COPY ({to_copy}) TO {copy_to} DELIMITER ',' CSV HEADER"
    logger.info("Copying to CSV: %s", cmd)
    result = run(cmd, capture_output=True, text=True, shell=True)
    if err := result.stderr:
        logger.error(err)
    if info := result.stdout:
        logger.debug(info)


def duckdb_copy_to_file(
    to_copy: Union[str, sa.Table, sa.select],
    save_path: str,
    pg_url: str,
    s3_cfg: Optional[S3Cfg] = None,
    table_name: Optional[str] = None,
):
    """Copy a table of query to a CSV or Parquet file.

    Args:
        to_copy (Union[str, sa.Table, sa.select]): _description_
        save_path (str): _description_
        pg_url (str): _description_
        s3_cfg (Optional[S3Cfg], optional): _description_. Defaults to None.
        table_name (Optional[str], optional): Schema-qualified table name. Needed only if `to_copy` is not a table. Defaults to None.
    """
    pg_db_name = pg_url.split("/")[-1]
    try:
        duckdb.execute(
            f"ATTACH '{remove_engine_driver(pg_url)}' AS {pg_db_name} (TYPE POSTGRES)"
        )
    except duckdb.BinderException as err:
        if f'database with name "{pg_db_name}" already exists' in str(err):
            logger.warning("Database is already attached: %s", pg_db_name)
        else:
            raise
    if isinstance(to_copy, (sa.Table, str)):
        to_copy = f"{pg_db_name}.{schema_table(to_copy)}"
    elif isinstance(to_copy, sa.Select):
        if table_name is None:
            raise ValueError(
                "If `to_copy` is not a table, `table_name` must be provided."
            )
        to_copy = compile_statement(to_copy)
        to_copy = re.sub(
            r"(?<!FROM\s)" + re.escape(f"{table_name}."), "", to_copy
        ).replace(table_name, f"{pg_db_name}.{table_name}")
    statement = f"COPY {to_copy} TO '{save_path}'"
    if ".csv" in save_path:
        statement += f" (HEADER, DELIMITER ',');"
    elif save_path.endswith(".parquet"):
        statement += f" (FORMAT PARQUET);"
    if is_s3_path(save_path):
        create_duckdb_secret(s3_cfg)
    else:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
    duckdb.execute(statement)
