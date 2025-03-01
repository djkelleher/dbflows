import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
from itertools import count
from pathlib import Path
from pprint import pformat
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import ClassVar, Generator, List, Optional, Sequence, Union

import pandas as pd
import sqlalchemy as sa
from fileflows import Files
from fileflows.s3 import S3Cfg, is_s3_path
from sqlalchemy import func as fn
from sqlalchemy.engine import Engine
from tqdm import tqdm

from dbflows.utils import engine_url, logger, range_slices
from dbflows.utils import schema_table as get_schema_table
from dbflows.utils import size_in_bytes, split_schema_table

from .utils import duckdb_copy_to_file, psql_copy_to_csv

SCHEMA_NAME = "exports"
SA_META = sa.MetaData(schema=SCHEMA_NAME)


def _create_file_name_re() -> re.Pattern:
    """path format: prefix_T(table)_P(partition)_slicestart_sliceend.csv.gz"""
    iso_date = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?"
    int_or_float = r"\d+(?:\.?\d+)?"
    slice_range = f"({iso_date}_{iso_date}|{int_or_float}_{int_or_float})"
    return re.compile(
        "".join(
            [
                # optional prefix: prefix_
                r"(?:(?P<prefix>\w*)_(?=T\())?"
                # schema table: T(schema.table)
                r"T\((?P<schema_table>\w+(?:\.\w+)?)\)",
                # optional partition: P(partition)
                r"(?:_P\((?P<partition>\w+)\))?",
                # optional slice range:
                f"(?:_(?P<slice_range>{slice_range}))?",
                # file suffix: .csv or .csv.gz
                r"(?P<suffix>\.csv(?:\.gz)?)$",
            ]
        )
    )


@dataclass
class ExportMeta:
    schema_table: str
    slice_start: Optional[Union[datetime, float, int]] = None
    slice_end: Optional[Union[datetime, float, int]] = None
    partition: Optional[str] = None
    prefix: Optional[str] = None
    statement: sa.select = None
    path: Optional[Path] = None

    _file_name_re: ClassVar[re.Pattern] = _create_file_name_re()

    @classmethod
    def from_file(cls, path: Union[str, Path]):
        """Construct Export from file name."""
        _, name = os.path.split(path)
        if not (params := ExportMeta._file_name_re.search(name)):
            raise ValueError(f"Invalid name format: {name}. Can not construct Export.")
        params = params.groupdict()
        table = params["schema_table"]
        if "." not in table:
            table = f"public.{table}"
        meta = cls(schema_table=table, prefix=params["prefix"], path=path)
        if partition := params["partition"]:
            meta.partition = partition.replace("@", "/")
        if slice_range := params["slice_range"]:
            slice_start, slice_end = slice_range.split("_")
            for var, val in (("slice_start", slice_start), ("slice_end", slice_end)):
                if val.isnumeric():
                    val = int(val)
                elif re.match(r"\d+\.\d+", val):
                    val = float(val)
                else:
                    val = datetime.fromisoformat(val)
                setattr(meta, var, val)
        return meta

    def create_file_name(self) -> str:
        """Create file name from ExportMeta parameters."""
        stem_parts = []
        if self.prefix:
            stem_parts.append(self.prefix)
        stem_parts.append(f"T({self.schema_table})")
        if self.partition:
            stem_parts.append(f"P({self.partition.replace('/', '@')})")
        if self.slice_start or self.slice_end:
            if not (self.slice_start and self.slice_end):
                raise ValueError("Both slice_start and slice_end are required.")
            if isinstance(self.slice_start, (datetime, date)):
                assert isinstance(self.slice_end, (datetime, date))
                stem_parts.append(
                    f"{self.slice_start.isoformat()}_{self.slice_end.isoformat()}"
                )
            else:
                assert isinstance(self.slice_start, (float, int))
                assert isinstance(self.slice_end, (float, int))
                stem_parts.append(f"{self.slice_start}_{self.slice_end}")
        return f"{'_'.join(stem_parts)}.csv.gz"

    def similar(self, **kwargs):
        # TODO check deepcopy of statement.
        return deepcopy(self).update(**kwargs)

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        return self


def export_append(
    table: Union[str, sa.Table],
    engine: Union[str, Engine],
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    slice_column: Optional[Union[str, sa.Column]] = None,
    partition_column: Optional[Union[str, sa.Column]] = None,
    file_max_size: Optional[str] = None,
    file_stem_prefix: Optional[str] = None,
    n_workers: Optional[int] = None,
    s3_cfg: Optional[S3Cfg] = None,
):
    """Export a database table.

    Args:
        table (sa.Table): The table or schema-qualified table name to export data from.
        engine: Union[str, Engine]: Engine or URL for the database to export data from.
        save_locs (Optional[ Union[Union[Path, str], Sequence[Union[Path, str]]] ], optional): Bucket(s) (format: s3://{bucket name}) and/or local director(y|ies) to save files to.
        slice_column (Optional[sa.Column], optional): A column with some kind of sequential ordering (e.g. time) that can be used sort rows within the table or a partition. Defaults to None.
        partition_column (Optional[sa.Column], optional): A column used to partition table data (e.g. a categorical value). Defaults to None.
        file_max_size (Optional[str], optional): Desired size of export files. Must have suffix 'bytes', 'kb', 'mb', 'gb', 'tb'. (e.g. '500mb'). If None, no limit will be placed on file size. Defaults to None.
        file_stem_prefix (Optional[str], optional): A prefix put on every export file name. Defaults to None.
        n_workers (Optional[int], optional): Number of export tasks to run simultaneously.
        s3_cfg (Optional[S3Cfg], optional): Parameters for s3. Defaults to None.
    """
    exports_meta = _create_export_meta(
        table=table,
        engine=engine,
        save_locs=save_locs,
        slice_column=slice_column,
        partition_column=partition_column,
        file_max_size=file_max_size,
        file_stem_prefix=file_stem_prefix,
        s3_cfg=s3_cfg,
    )
    append = slice_column is not None
    if slice_column is None and partition_column is None and file_max_size is None:
        # entire table is being exported.
        exports = list(exports_meta)
        assert len(exports) == 1
        _export(
            meta=exports[0],
            engine=engine,
            append=append,
            save_locs=save_locs,
            s3_cfg=s3_cfg,
        )
    else:
        # run all export tasks.
        _run_export_tasks(
            engine=engine,
            exports=exports_meta,
            append=append,
            save_locs=save_locs,
            n_workers=n_workers,
            s3_cfg=s3_cfg,
        )


def export_hypertable_chunks(
    engine: Union[str, Engine],
    table: Union[sa.Table, str],
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    n_workers: Optional[int] = None,
    s3_cfg: Optional[S3Cfg] = None,
):
    """Export all chunks of a TimescaleDB hypertable. (one file per chunk)

    Args:
        engine (Union[str, Engine]): Engine or URL for the database to export data from.
        table (Union[sa.Table, str]): The table or schema-qualified table name to export data from.
        save_locs (Union[Union[Path, str], Sequence[Union[Path, str]]]): Bucket(s) (format: s3://{bucket name}) and/or local director(y|ies) to save files to.
        n_workers (Optional[int], optional): Number of export tasks to run simultaneously. Defaults to None.
        fo (Optional[Files], optional): File operations interface for s3/local disk. Defaults to None.
    """
    if isinstance(engine, str):
        engine = sa.create_engine(engine)
    if not isinstance(table, str):
        table = get_schema_table(table)
    with engine.begin() as conn:
        chunks = (
            conn.execute(sa.text(f"SELECT show_chunks('{table}');")).scalars().all()
        )
    exports = [
        ExportMeta(
            schema_table=chunk, statement=sa.select("*").select_from(sa.text(chunk))
        )
        for chunk in chunks
    ]
    _run_export_tasks(
        engine,
        exports,
        append=False,
        save_locs=save_locs,
        n_workers=n_workers,
        s3_cfg=s3_cfg,
    )


def _export(
    meta: ExportMeta,
    engine: Engine,
    append: bool,
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    s3_cfg: Optional[S3Cfg] = None,
):
    """Export data as specified in `meta`."""
    fo = Files(s3_cfg=s3_cfg)
    save_locs = [save_locs] if isinstance(save_locs, (str, Path)) else save_locs
    primary_save_loc, *backup_save_locs = save_locs
    for loc in save_locs:
        fo.create(loc)
    # file name generated from meta attributes.
    created_file_name = meta.create_file_name()
    prim_loc_file = f"{primary_save_loc}/{created_file_name}"
    # check if we are appending to an existing file.
    if meta.path:
        _, path_name = os.path.split(meta.path)
        # if appending to file and it's in s3, we need to move it to a local folder.
        if is_s3_path(meta.path):
            save_path = f"{TemporaryDirectory().name}/{path_name}"
            fo.move(meta.path, save_path)
        else:
            # the file being appended to is already a local file.
            save_path = meta.path
    # if primary save location is s3, we need to save to a local file first, then move it.
    # elif is_s3_path(primary_save_loc):
    #    save_path = f"{TemporaryDirectory().name}/{created_file_name}"
    else:
        # primary save location is a local file, so save directly to primary location.
        save_path = prim_loc_file
    if append:
        psql_copy_to_csv(
            to_copy=meta.statement, engine=engine, save_path=save_path, append=append
        )
    else:
        duckdb_copy_to_file(
            to_copy=meta.statement,
            table_name=meta.schema_table,
            save_path=save_path,
            pg_url=engine_url(engine),
            s3_cfg=s3_cfg,
        )
    save_path_head, save_path_name = os.path.split(save_path)
    # If we appended to existing file and the file name includes end time, then the file name will need to be updated.
    if save_path_name != created_file_name:
        logger.info(
            "Renaming appended file: %s -> %s", save_path_name, created_file_name
        )
        new_save_path = f"{save_path_head}/{created_file_name}"
        fo.move(save_path, new_save_path)
        save_path = new_save_path
    # copy new file to backup locations.
    for bac_loc in backup_save_locs:
        file = f"{bac_loc}/{created_file_name}"
        fo.copy(save_path, file)
        logger.info("Saved %s", file)
    # move to primary save location if needed.
    if save_path != prim_loc_file:
        fo.move(save_path, prim_loc_file)
    logger.info("Saved %s", prim_loc_file)
    # clean up old files we don't need anymore.
    if meta.path and path_name != created_file_name:
        logger.info("Deleting old file: %s", meta.path)
        fo.delete(meta.path, if_exists=True)
        # remove old backup files.
        for bac_loc in backup_save_locs:
            bac_file = f"{bac_loc}/{path_name}"
            logger.info("Deleting old file: %s", bac_file)
            fo.delete(bac_file)


def _run_export_tasks(
    engine: Engine,
    exports: Sequence[ExportMeta],
    append: bool,
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    n_workers: Optional[int] = None,
    s3_cfg: Optional[S3Cfg] = None,
):
    """Export a data file for each `ExportMeta` in `exports`."""

    if n_workers is None:
        n_workers = int(os.cpu_count() * 0.8)
    logger.info("Starting exports with %i workers", n_workers)
    with ThreadPoolExecutor(max_workers=n_workers) as p:
        tasks = [
            p.submit(
                _export,
                meta=meta,
                engine=engine,
                append=append,
                save_locs=save_locs,
                s3_cfg=s3_cfg,
            )
            for meta in exports
        ]
        n_tasks = len(tasks)
        logger.info("Waiting for %i export tasks to finish...", n_tasks)
        n_complete = count(1)
        for task in as_completed(tasks):
            task.result()
            logger.info("Finished export %i/%i", next(n_complete), n_tasks)


def get_table_row_size_table(engine: Engine) -> sa.Table:
    """Get table_row_sizes, creating schema/table if needed."""
    table_name = "table_row_sizes"
    if (table := SA_META.tables.get(f"{SCHEMA_NAME}.{table_name}")) is None:
        table = sa.Table(
            table_name,
            SA_META,
            sa.Column(
                "table", sa.Text, primary_key=True, comment="Format: {schema}.{table}"
            ),
            sa.Column(
                "row_bytes",
                sa.Integer,
                comment="Number of bytes of an average row in the table.",
            ),
        )
    with engine.begin() as conn:
        if not engine.dialect.has_schema(conn, schema=SCHEMA_NAME):
            conn.execute(sa.schema.CreateSchema(SCHEMA_NAME))
    table.create(engine, checkfirst=True)
    return table


def target_export_row_count(
    table: sa.Table,
    export_size: int,
    engine: Engine,
    min_row_sample_size: int = 10_000,
    desired_row_sample_size: int = 100_000,
) -> int:
    """Find approximately how many rows will equal the desired file size.

    Args:
        table (sa.Table): The table who's data will be exported.
        export_size (int): Desired file size in bytes.
    """
    row_sizes_table = get_table_row_size_table(engine)
    schema_table = get_schema_table(table)
    with engine.begin() as conn:
        row_bytes = conn.execute(
            sa.select(
                row_sizes_table.c.row_bytes,
            ).where(row_sizes_table.c.table == schema_table)
        ).scalar()
        n_rows = conn.execute(sa.select(sa.func.count()).select_from(table)).scalar()

    if row_bytes is not None:
        row_count = round(export_size / row_bytes)
    elif n_rows < min_row_sample_size:
        # use all rows because we don't have enough to calculate sample statistics.
        row_count = n_rows
    else:
        sample_n_rows = min(n_rows, desired_row_sample_size)
        logger.info(
            "Determining export row count for table %s. Using sample size %i",
            table.name,
            sample_n_rows,
        )
        sample_file = Path(NamedTemporaryFile().name).with_suffix(".csv.gz")
        query = sa.select(table)
        if n_rows > sample_n_rows:
            query = query.limit(sample_n_rows)
        psql_copy_to_csv(
            to_copy=query,
            save_path=sample_file,
            engine=engine,
            append=False,
        )
        sample_bytes = os.path.getsize(sample_file)
        logger.info(
            "Created sample file for table %s (%i bytes).",
            table.name,
            sample_bytes,
        )
        row_bytes = sample_bytes / sample_n_rows
        logger.info("Determined table %s row bytes: %i.", schema_table, row_bytes)
        with engine.begin() as conn:
            conn.execute(
                sa.insert(row_sizes_table).values(
                    {
                        "table": schema_table,
                        "row_bytes": row_bytes,
                    }
                )
            )
        row_count = round(export_size / row_bytes)
    logger.info("Exporting %i rows per file for table %s.", row_count, schema_table)
    return row_count


def _create_export_meta(
    table: Union[str, sa.Table],
    engine: Union[str, Engine],
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    slice_column: Optional[Union[str, sa.Column]] = None,
    partition_column: Optional[Union[str, sa.Column]] = None,
    file_max_size: Optional[str] = None,
    file_stem_prefix: Optional[str] = None,
    s3_cfg: Optional[S3Cfg] = None,
) -> Generator[None, ExportMeta, None]:
    if isinstance(table, str):
        schema_table = table if "." in table else f"public.{table}"
        schema, table_name = split_schema_table(schema_table)
    else:
        schema = table.schema
        table_name = table.name
        schema_table = get_schema_table(table)
    if isinstance(engine, str):
        engine = sa.create_engine(engine)
    # make sure table being exported exists.
    with engine.begin() as conn:
        if not engine.dialect.has_table(conn, table_name, schema=schema):
            raise RuntimeError(
                f"Can not export table {table_name}. Table does not exist."
            )
    if isinstance(table, str):
        table = sa.Table(
            table_name,
            sa.MetaData(schema=schema),
            autoload_with=engine,
        )

    if not isinstance(save_locs, (list, tuple, set)):
        save_locs = [save_locs]
    fo = Files(s3_cfg=s3_cfg)
    # make sure columns are SQLAlchemy columns.
    if isinstance(slice_column, str):
        slice_column = table.columns[slice_column]
    # make sure slice column is a supported type.
    if slice_column is not None and slice_column.type.python_type not in (
        datetime,
        int,
        float,
    ):
        raise ValueError(
            f"Unsupported `slice_column` type ({type(slice_column)}): {slice_column}"
        )
    if isinstance(partition_column, str):
        partition_column = table.columns[partition_column]
    file_max_size = size_in_bytes(file_max_size) if file_max_size else None
    primary_save_loc = save_locs[0]
    if slice_column is not None:
        # we will append to existing files if they exist, so load existing export files from primary save location for this table.
        if (
            primary_save_loc
            and fo.exists(primary_save_loc)
            and (files := fo.list_files(primary_save_loc))
        ):
            logger.info(
                "Found %i existing export files in primary save location %s.",
                len(files),
                primary_save_loc,
            )
            existing_table_exports = [ExportMeta.from_file(f) for f in files]
            # Files in the primary save location that have same parameters as this instance.
            existing_table_exports = [
                f
                for f in existing_table_exports
                if f.prefix == file_stem_prefix and f.schema_table == schema_table
            ]
            # sort by date, newest to oldest.
            existing_table_exports.sort(key=lambda x: x.slice_end or 0, reverse=True)
        else:
            logger.info(
                "No existing export files found in primary save location %s.",
                primary_save_loc,
            )
            existing_table_exports = []

        if file_max_size is not None:
            file_target_row_count = target_export_row_count(
                table=table,
                export_size=file_max_size,
                engine=engine,
            )

    def latest_export_file(partition: Optional[str] = None) -> ExportMeta:
        """Find the most recent export file for a table or table partition."""
        if partition:
            # return the newest file for the partition.
            for file in existing_table_exports:
                if file.partition == partition:
                    logger.info(
                        "Appending to last export file for partition %s: %s.",
                        partition,
                        pformat(file),
                    )
                    return file
        elif existing_table_exports:
            # return the newest file.
            file = existing_table_exports[0]
            logger.info("Appending to last export file: %s.", file)
            return file

    def export_file_common() -> ExportMeta:
        """Common attributes shared by potentially multiple export files."""
        statement = sa.select(table).select_from(table)
        if slice_column is not None:
            statement = statement.order_by(slice_column.asc())
        return ExportMeta(
            schema_table=str(schema_table),
            prefix=file_stem_prefix,
            statement=statement,
        )

    def build_export_files(partition: Optional[str] = None) -> List[ExportMeta]:
        """Construct ExportMeta for each needed query."""
        export_file = export_file_common()

        if partition:
            export_file.partition = partition
            export_file.statement = export_file.statement.where(
                partition_column == partition
            )

        if slice_column is None:
            # can't split file if not using slice column. must export all rows.
            return [export_file]

        slice_start_query = sa.select(fn.min(slice_column))
        slice_end_query = sa.select(fn.max(slice_column))
        if partition:
            slice_start_query = slice_start_query.where(partition_column == partition)
            slice_end_query = slice_end_query.where(partition_column == partition)
        with engine.begin() as conn:
            slice_start = conn.execute(slice_start_query).scalar()
            slice_end = conn.execute(slice_end_query).scalar()

        # check if data was previously exported for this configuration.
        last_export_meta = latest_export_file(partition)

        if file_max_size is None:
            if last_export_meta:
                # append everything new in the database to the last export file.
                if last_export_meta.slice_end is None:
                    # file was previously exported without specifying slice column (and hence slice end was not parsed from file stem)
                    last_export_meta.slice_end = pd.read_csv(
                        last_export_meta.path, names=[c.name for c in table.columns]
                    )[slice_column.name].max()
                return [
                    last_export_meta.similar(
                        statement=export_file.statement.where(
                            slice_column > last_export_meta.slice_end
                        ),
                        slice_start=slice_start,
                        slice_end=slice_end,
                    )
                ]
            # no previous export and no file size limit, so export all data.
            export_file.slice_start = slice_start
            export_file.slice_end = slice_end
            return [export_file]
        # construct meta for slice files.
        table_n_rows_query = sa.select(fn.count()).select_from(table)
        export_files = []

        # there is a file size limit, so split data into files of size file_max_size.
        if last_export_meta:
            # find number of rows that should be appended to the last export file.
            n_rows_needed = file_target_row_count * (
                1 - (fo.file_size(last_export_meta.path) / file_max_size)
            )
            if n_rows_needed > 0:
                with engine.begin() as conn:
                    last_export_new_slice_end = conn.execute(
                        sa.select(fn.max(sa.text("slice_column"))).select_from(
                            sa.select(slice_column.label("slice_column"))
                            .where(slice_column > last_export_meta.slice_end)
                            .order_by(slice_column.asc())
                            .limit(n_rows_needed)
                            .subquery()
                        )
                    ).scalar()
                export_files.append(
                    last_export_meta.similar(
                        statement=export_file.statement.where(
                            slice_column > last_export_meta.slice_end,
                            slice_column <= last_export_new_slice_end,
                        ),
                        slice_end=last_export_new_slice_end,
                    )
                )
                slice_start = last_export_new_slice_end
            else:
                slice_start = last_export_meta.slice_end
            table_n_rows_query = table_n_rows_query.where(slice_column > slice_start)

        if slice_start == slice_end:
            # no more data to export.
            return export_files

        # add new queries for any remaining data.
        with engine.begin() as conn:
            n_rows = conn.execute(table_n_rows_query).scalar()

        query_bounds = range_slices(
            start=slice_start,
            end=slice_end,
            periods=round(n_rows / file_target_row_count),
        )
        first_query_start, first_query_end = query_bounds[0]
        query_meta = export_file.similar(
            slice_start=first_query_start,
            slice_end=first_query_end,
        )
        if last_export_meta:
            query_meta.statement = export_file.statement.where(
                slice_column > first_query_start,
                slice_column <= first_query_end,
            )
        else:
            query_meta.statement = export_file.statement.where(
                slice_column <= first_query_end
            )
        export_files.append(query_meta)

        # create queries for remaining data.
        for start, end in query_bounds[1:]:
            export_files.append(
                export_file.similar(
                    slice_start=start,
                    slice_end=end,
                    statement=export_file.statement.where(
                        slice_column > start, slice_column <= end
                    ),
                )
            )
        return export_files

    # Construct queries and metadata for exports.
    if partition_column is None:
        logger.info("Preparing export tasks...")
        if slice_column is not None:
            for file_export in build_export_files():
                yield file_export
        else:
            yield export_file_common()
    else:
        with engine.begin() as conn:
            partitions = list(
                conn.execute(sa.select(partition_column).distinct()).scalars()
            )
        logger.info("Preparing export tasks for %i partitions...", len(partitions))
        for partition in tqdm(partitions):
            for export_file in build_export_files(partition):
                yield export_file
