import os
import re
from collections.abc import Sequence as ABCSequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from subprocess import run
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Literal, Optional, Sequence, Union

import duckdb
import sqlalchemy as sa
from fileflows import Files, S3Cfg, create_duckdb_secret, is_s3_path
from pydantic import BaseModel, PrivateAttr, field_validator, model_validator
from sqlalchemy.engine import Engine

from dbflows.utils import (compile_statement, engine_url, logger,
                           remove_engine_driver, schema_table,
                           split_schema_table)

from .duck import mount_pg_db


@dataclass
class ExportLocation:
    """Location to save export data file to."""

    save_path: Union[Path, str]
    # S3 credentials (if save_path is an S3  path)
    s3_cfg: Optional[S3Cfg] = None

    def __post_init__(self):
        self.save_path = str(self.save_path)
        self.files = Files(s3_cfg=self.s3_cfg)

    @classmethod
    def from_table_and_type(
        cls,
        table: str,
        file_type: Literal["csv", "csv.gz", "parquet"],
        save_path: Union[Path, str],
        s3_cfg: Optional[S3Cfg] = None,
    ):
        """
        Create an ExportLocation instance from a table name and file type.

        Args:
            table (str): The name of the table to export.
            file_type (Literal["csv", "csv.gz", "parquet"]): The type of file to save the data as.
            save_path (Union[Path, str]): The location (path or URL) to save the exported file.
            s3_cfg (Optional[S3Cfg]): Optional S3 configuration for saving to an S3 bucket.

        Returns:
            ExportLocation: An instance of ExportLocation with the specified parameters.
        """
        file_name = "/".join(split_schema_table(schema_table(table))) + f".{file_type}"
        return cls(save_path=f"{save_path}/{file_name}", s3_cfg=s3_cfg)

    @property
    def is_s3(self) -> bool:
        """
        Check if the export location is an S3 path.

        Returns:
            bool: True if the export location is an S3 path, False otherwise.
        """
        return is_s3_path(self.save_path)
    
    def exists(self) -> bool:
        """
        Check if the export location file exists.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        return self.files.exists(self.save_path)

    def create(self):
        """
        Create the export location (if it does not already exist).

        This method creates the necessary directories or paths for the export location,
        either locally or on an S3 bucket, using the `Files` utility.

        Returns:
            None
        """
        self.files.create(self.save_path)


class Export(BaseModel):
    """Base class for all export operations."""
    # A sequence of strings or ExportLocation objects representing the locations to save the exported table.
    export_locations: Union[Union[str, ExportLocation],Sequence[Union[str, ExportLocation]]]
    # The PostgreSQL connection URL.
    pg_url: str
    # The type of file to save the data as. If not specified, the file type will be determined by the file extension of the first export location.
    file_type: Optional[Literal["csv", "parquet"]] = None
    # Optional compression level for saving to a parquet file.
    compression_level: Optional[int] = None
    # private internal vars.
    _is_table: bool = PrivateAttr(None)
    _to_copy: Union[str, sa.Table] = PrivateAttr(None)
    _partition_by: Optional[str] = PrivateAttr(None)
    
    model_config = {
        "arbitrary_types_allowed": True  # To allow SQLAlchemy types
    }

    @field_validator('export_locations')
    def process_export_locations(cls, export_locations: Any) -> Sequence[ExportLocation]:
        # Handle non-sequence input (single string or ExportLocation)
        if not isinstance(export_locations, (list, tuple)):
            export_locations = [export_locations]
        
        # Convert strings to ExportLocation objects
        return [ExportLocation(save_path=loc) if isinstance(loc, str) else loc for loc in export_locations]


class TableExport(Export):
    """Copy a database table to a file."""
    # The table or schema-qualified table name to export data from.
    table: Union[str, sa.Table]

    @model_validator(mode='after')
    def setup_fields(self):
        self._is_table = True
        self._to_copy = self.table
        return self


class QueryExport(Export):
    """Copy the results of a query to a file."""
    # SQL query or sqlalchemy Select object.
    query: Union[str, sa.Select]
    table_name: str  # Required for _compile_query

    @model_validator(mode='after')
    def setup_fields(self):
        self._is_table = False
        self._to_copy = _compile_query(self.query, self.pg_url, self.table_name)
        return self

class TablePartitionExport(Export):
    """Copy the contents of a table to a file, partitioned by a column."""
    table: Union[str, sa.Table]
    partition_by: str

    @model_validator(mode='after')
    def setup_fields(self):
        self._is_table = True
        self._to_copy = self.table
        self._partition_by = self.partition_by
        return self


class QueryPartitionExport(Export):
    """Copy the results of a query to a file, partitioned by a column."""
    query: Union[str, sa.Select]
    partition_by: str
    table_name: str
    
    @model_validator(mode='after')
    def setup_fields(self):
        self._is_table = False
        self._to_copy = _compile_query(self.query, self.pg_url, self.table_name)
        self._partition_by = self.partition_by
        return self


def run_exports(exports: List[Export], n_workers: Optional[int] = None):
    n_workers = n_workers or max(os.cpu_count() // 2, 1)
    logger.info("Running %i exports with %i workers", len(exports), n_workers)
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        executor.map(run_export, exports)


def run_export(export: Export):
    """
    Export database data to a file.

    This function will export the specified table to files of the same type as
    the specified export locations. If multiple locations are specified with the
    same file type, the table will only be exported once and then copied to all
    of the locations.

    If any of the export locations are local directories, the table will be
    exported to one of the local directories and then copied to the other
    locations. If no local directories are specified, the table will be exported
    to a temporary file and then copied to all of the locations.
    """
    # Create all export locations (if they do not already exist).
    for loc in export.export_locations:
        loc.create()
    with NamedTemporaryFile() as tf:
        # check if any save location is a local directory.
        local_dirs = [l for l in export.export_locations if not l.is_s3]
        if local_dirs:
            # if there is a local directory, use it as the export location.
            export_loc = local_dirs.pop(0)
            # the rest of the locations are just for copying.
            copy_locs = local_dirs + [l for l in export.export_locations if l.is_s3]
        elif len(export.export_locations) > 1:
            # if there is no local directory, export to a temporary file
            # and then copy to all of the locations.
            export_loc = tf
            copy_locs = export.export_locations
        else:
            assert len(export.export_locations) == 1
            # if there is only one location, use it as the export location.
            export_loc = export.export_locations[0]
            # there are no other locations to copy to.
            copy_locs = []
        # export the table to the export location.
        _duckdb_copy(
            to_copy=export._to_copy,
            is_table=export._is_table,
            save_path=export_loc.save_path,
            pg_url=export.pg_url,
            file_type=export.file_type,
            partition_by=export._partition_by,
            compression_level=export.compression_level,
            s3_cfg=export_loc.s3_cfg,
        )
        # copy the exported file to the other locations.
        for loc in copy_locs:
            loc.files.copy(export_loc.save_path, loc.save_path)


def _duckdb_copy(
    to_copy: Union[str, sa.Table],
    is_table: bool,
    save_path: str,
    pg_url: str,
    partition_by: Optional[str] = None,
    compression_level: Optional[int] = None,
    file_type: Optional[Literal["csv", "parquet"]] = None,
    s3_cfg: Optional[S3Cfg] = None,
):
    # Get the name of the PostgreSQL database
    pg_db_name = pg_url.split("/")[-1]

    # If we're copying a table, prepend the database name to the table name
    if is_table:
        to_copy = f"{pg_db_name}.{schema_table(to_copy)}"

    # Set up the COPY statement arguments
    args = []

    # If we're saving to a CSV file, add the necessary arguments
    if (
        file_type == "csv"
        or save_path.endswith(".csv")
        or save_path.endswith(".csv.gz")
    ):
        args.append("HEADER, DELIMITER ','")
    # If we're saving to a Parquet file, add the necessary argument
    elif file_type == "parquet" or save_path.endswith(".parquet"):
        args.append("FORMAT PARQUET")
        if compression_level:
            args.append(f"COMPRESSION 'zstd', COMPRESSION_LEVEL {min(compression_level, 22)}")
    # If we don't support the file type, raise an error
    else:
        raise ValueError(f"Unsupported file type: {save_path}")

    # If we're partitioning the data, add the necessary argument
    if partition_by:
        args.append(f"PARTITION_BY ({partition_by})")

    # Construct the COPY statement
    statement = f"COPY {to_copy} TO '{save_path}' ({','.join(args)});"

    # Connect to the DuckDB database
    with duckdb.connect() as conn:
        mount_pg_db(pg_url=pg_url, conn=conn)
        # If we're saving to an S3 bucket, create the necessary secret
        if is_s3_path(save_path):
            create_duckdb_secret(s3_cfg, conn=conn)
        # Otherwise, create the local directory if it doesn't exist
        else:
            Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        # Execute the COPY statement
        conn.execute(statement)


def _compile_query(query: str, pg_db: str, table_name: str) -> str:
    """
    Prepend database name to table name in a query string.

    Parameters
    ----------
    query : str
        The query string to modify.
    pg_db : str
        The name of the PostgreSQL database.
    table_name : str
        The name of the table to modify in the query.

    Returns
    -------
    str
        The modified query string.
    """
    pg_db_name = pg_db.split("/")[-1] if "/" in pg_db else pg_db
    if isinstance(query, sa.Select):
        # If the query is a sqlalchemy Select object, convert it to a string
        query = compile_statement(query)
    # Replace the table name with the full name including the database name,
    # but only outside of FROM statements
    return re.sub(r"(?<!FROM\s)" + re.escape(f"{table_name}."), "", query).replace(
        table_name, f"{pg_db_name}.{table_name}"
    )


def psql_copy_to_csv(
    to_copy: Union[str, sa.Table, sa.select],
    save_path: Union[Path, str],
    engine: Union[str, Engine],
    append: bool,
):
    """
    Copy data from a database to a CSV file.

    Parameters
    ----------
    to_copy : str or sqlalchemy.Table or sqlalchemy.select
        The data to copy from the database.
    save_path : Path or str
        The path where the CSV file should be saved.
    engine : str or sqlalchemy.Engine
        The database connection string or an Engine instance.
    append : bool
        Whether to append to an existing file or overwrite it.

    Returns
    -------
    None
    """
    # Get the string representation of the table to copy
    to_copy = (
        schema_table(to_copy)
        if isinstance(to_copy, sa.Table)
        else f"({compile_statement(to_copy)})"
    )

    # Get the path to the file that we are saving to
    save_path = Path(save_path)

    # If we are not appending to a file, we want to completely overwrite
    # any existing file. Otherwise, we will append to the existing file.
    if not save_path.exists() and save_path.suffix != ".gz":
        copy_to = f"'{save_path}'"
    else:
        # If the file already exists, we use the "cat" program to append
        # to the existing file. If the file is compressed, we use gzip
        # to decompress it and append to the uncompressed file.
        program = "gzip" if save_path.suffix == ".gz" else "cat"
        operator = ">>" if append else ">"
        copy_to = f"""PROGRAM '{program} {operator} "{save_path}"'"""

    # Make sure the directory exists
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # Get the database connection string without the driver
    db_url = remove_engine_driver(engine_url(engine))

    # Construct the psql command to copy the data
    psql_code = " ".join(
        [r"\copy", to_copy, "TO", copy_to, "DELIMITER ',' CSV"]
    ).replace('"', '\\"')

    # Construct the command to run psql
    cmd = f"""psql "{db_url}" -c "{psql_code}\""""

    # Log some information about the command
    logger.info("Copying to CSV: %s", cmd)

    # Run the command
    result = run(cmd, capture_output=True, text=True, shell=True)

    # Log any errors
    if err := result.stderr:
        logger.error(err)

    # Log any output
    if info := result.stdout:
        logger.debug(info)
