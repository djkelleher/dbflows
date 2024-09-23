import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from subprocess import run
from tempfile import NamedTemporaryFile
from typing import Literal, Optional, Sequence, Union

import duckdb
import sqlalchemy as sa
from fileflows import Files, S3Cfg, create_duckdb_secret, is_s3_path
from fileflows.s3 import S3Cfg, is_s3_path
from sqlalchemy.engine import Engine

from dbflows.utils import (
    compile_statement,
    engine_url,
    logger,
    remove_engine_driver,
    schema_table,
    split_schema_table,
)


@dataclass
class ExportLocation:
    """Location to save export data file to."""

    save_path: Union[Path, str]
    # S3 credentials (if save_loc is an S3  path)
    s3_cfg: Optional[S3Cfg] = None

    def __post_init__(self):
        self.save_loc = str(self.save_loc)
        self.files = Files(s3_cfg=self.s3_cfg)

    @classmethod
    def from_table_and_type(
        cls,
        table: str,
        file_type: Literal["csv", "csv.gz", "parquet"],
        save_loc: Union[Path, str],
        s3_cfg: Optional[S3Cfg] = None,
    ):
        file_name = "/".join(split_schema_table(schema_table(table))) + f".{file_type}"
        return cls(save_path=f"{save_loc}/{file_name}", s3_cfg=s3_cfg)

    @property
    def is_s3(self) -> bool:
        return is_s3_path(self.save_path)

    def create(self):
        """make sure bucket or local directory exists."""
        self.files.create(self.save_path)


@dataclass
class ExportLocations:
    def __init__(self, export_locations: Sequence[Union[str, ExportLocation]]):
        if not isinstance(export_locations, (list, tuple)):
            export_locations = [export_locations]
        self.export_locations = [
            ExportLocation(loc) if isinstance(loc, str) else loc
            for loc in export_locations
        ]

    def create(self):
        for loc in self.export_locations:
            loc.create()

    def file_type_locations(self):
        ftl = defaultdict(list)
        for loc in self.export_locations:
            ftl[loc.save_path.suffix].append(loc)
        return ftl

    @staticmethod
    def local_dirs(export_locations: Sequence[ExportLocation]):
        return [l for l in export_locations if not l.is_s3]

    @staticmethod
    def s3_dirs(export_locations: Sequence[ExportLocation]):
        return [l for l in export_locations if l.is_s3]


def export_table_to_file(
    table: Union[str, sa.Table],
    pg_url: str,
    export_locations: Sequence[Union[str, ExportLocation]],
):
    """Export a table to a file."""
    export_locations = ExportLocations(export_locations)
    with NamedTemporaryFile() as tf:
        # run for each file type (if multiple)
        for suffix, locations in export_locations.file_type_locations.items():
            # check if any save location is a local directory.
            local_dirs = export_locations.local_dirs(locations)
            if local_dirs:
                export_loc = local_dirs.pop(0)
                copy_locs = local_dirs + export_locations.s3_dirs(locations)
            elif len(export_locations) > 1:
                export_loc = tf
                copy_locs = export_locations
            else:
                assert len(export_locations) == 1
                export_loc = export_locations[0]
                copy_locs = []
            duckdb_copy_table_to_file(
                table=table,
                save_path=export_loc.save_path,
                pg_url=pg_url,
                file_type=suffix,
                s3_cfg=export_loc.s3_cfg,
            )
            # copy to other locations.
            for loc in copy_locs:
                loc.files.copy(export_loc.save_path, loc.save_path)


def duckdb_copy_query_to_partition(
    query: Union[str, sa.Select],
    save_path: str,
    pg_url: str,
    partition_by: str,
    file_type: Literal["csv", "parquet"],
    s3_cfg: Optional[S3Cfg] = None,
    table_name: Optional[str] = None,
):
    _duckdb_copy(
        to_copy=_compile_query(query, pg_url, table_name),
        is_table=False,
        save_path=save_path,
        pg_url=pg_url,
        s3_cfg=s3_cfg,
        file_type=file_type,
        partition_by=partition_by,
    )


def duckdb_copy_table_to_partition(
    table: Union[str, sa.Table],
    save_path: str,
    pg_url: str,
    partition_by: str,
    file_type: Literal["csv", "parquet"],
    s3_cfg: Optional[S3Cfg] = None,
):
    _duckdb_copy(
        to_copy=table,
        is_table=True,
        save_path=save_path,
        pg_url=pg_url,
        s3_cfg=s3_cfg,
        file_type=file_type,
        partition_by=partition_by,
    )


def duckdb_copy_query_to_file(
    query: Union[str, sa.Select],
    save_path: str,
    pg_url: str,
    table_name: str,
    file_type: Optional[Literal["csv", "parquet"]] = None,
    s3_cfg: Optional[S3Cfg] = None,
):
    _duckdb_copy(
        to_copy=_compile_query(query, pg_url, table_name),
        is_table=False,
        save_path=save_path,
        pg_url=pg_url,
        file_type=file_type,
        s3_cfg=s3_cfg,
    )


def duckdb_copy_table_to_file(
    table: Union[str, sa.Table],
    save_path: str,
    pg_url: str,
    file_type: Optional[Literal["csv", "parquet"]] = None,
    s3_cfg: Optional[S3Cfg] = None,
):
    """Copy a table to a CSV or Parquet file.

    Args:
        table (Union[str, sa.Table]): Table to export.
        save_path (str): File path where table should be saved.
        pg_url (str): Postgresql URL
        s3_cfg (Optional[S3Cfg], optional): _description_. Defaults to None.
    """
    _duckdb_copy(
        to_copy=table,
        is_table=True,
        save_path=save_path,
        pg_url=pg_url,
        file_type=file_type,
        s3_cfg=s3_cfg,
    )


def mount_pg_db(pg_url: str, conn=None) -> str:
    pg_db_name = pg_url.split("/")[-1]
    conn = conn or duckdb
    try:
        conn.execute(
            f"ATTACH '{remove_engine_driver(pg_url)}' AS {pg_db_name} (TYPE POSTGRES)"
        )
    except duckdb.BinderException as err:
        if f'database with name "{pg_db_name}" already exists' in str(err):
            logger.warning("Database is already attached: %s", pg_db_name)
        else:
            raise
    return pg_db_name


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


def _duckdb_copy(
    to_copy: Union[str, sa.Table],
    is_table: bool,
    save_path: str,
    pg_url: str,
    partition_by: Optional[str] = None,
    file_type: Optional[Literal["csv", "parquet"]] = None,
    s3_cfg: Optional[S3Cfg] = None,
):
    pg_db_name = mount_pg_db(pg_url)
    if is_table:
        to_copy = f"{pg_db_name}.{schema_table(to_copy)}"
    args = []
    if (
        file_type == "csv"
        or save_path.endswith(".csv")
        or save_path.endswith(".csv.gz")
    ):
        args += ["HEADER", "DELIMITER ','"]
    elif file_type == "parquet" or save_path.endswith(".parquet"):
        args.append("FORMAT PARQUET")
    else:
        raise ValueError(f"Unsupported file type: {save_path}")
    if partition_by:
        args.append(f"PARTITION_BY ({partition_by})")
    statement = f"COPY {to_copy} TO '{save_path}' ({','.join(args)});"
    conn = duckdb.connect()
    if is_s3_path(save_path):
        create_duckdb_secret(s3_cfg, conn=conn)
    else:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
    conn.execute(statement)
    conn.close()


def _compile_query(query: str, pg_db: str, table_name: str) -> str:
    """Prepend database name to table name where needed."""
    pg_db_name = pg_db.split("/")
    if isinstance(query, sa.Select):
        query = compile_statement(query)
    return re.sub(r"(?<!FROM\s)" + re.escape(f"{table_name}."), "", query).replace(
        table_name, f"{pg_db_name}.{table_name}"
    )
