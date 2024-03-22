import os
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from pprint import pformat
from tempfile import NamedTemporaryFile
from typing import ClassVar, List, Optional, Sequence, Union

import pandas as pd
import sqlalchemy as sa
from fileflows import Files
from sqlalchemy import func as fn
from sqlalchemy.engine import Engine
from tqdm import tqdm

from .utils import copy_to_csv, logger, range_slices
from .utils import schema_table as get_schema_table
from .utils import split_schema_table

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


def size_in_bytes(mem_size: str) -> int:
    """Convert a memory size string to bytes."""
    if not (n_units := re.search(r"\d+(?:\.\d+)?", mem_size)):
        raise ValueError(f"Could not parse memory size: {mem_size}")
    n_units = n_units.group()
    unit_type = mem_size.replace(n_units, "").strip().lower()
    if unit_type in ("b", "byte", "bytes"):
        return int(n_units)
    if unit_type == "kb":
        exp = 1
    elif unit_type == "mb":
        exp = 2
    elif unit_type == "gb":
        exp = 3
    elif unit_type == "tb":
        exp = 4
    else:
        raise ValueError(f"Unknown memory unit: {unit_type}")
    return round(float(n_units) * 1000**exp)


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
        copy_to_csv(
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


def create_export_meta(
    table: Union[str, sa.Table],
    engine: Union[str, Engine],
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    slice_column: Optional[Union[str, sa.Column]] = None,
    partition_column: Optional[Union[str, sa.Column]] = None,
    file_max_size: Optional[str] = None,
    file_stem_prefix: Optional[str] = None,
    fo: Optional[Files] = None,
) -> List[ExportMeta]:
    """Export data from a database table.

    Args:
        table (sa.Table): The table or schema-qualified table name to export data from.
        save_locs (Optional[ Union[Union[Path, str], Sequence[Union[Path, str]]] ], optional): Bucket(s) (format: s3://{bucket name}) and/or local director(y|ies) to save files to.
        slice_column (Optional[sa.Column], optional): A column with some kind of sequential ordering (e.g. time) that can be used sort rows within the table or a partition. Defaults to None.
        partition_column (Optional[sa.Column], optional): A column used to partition table data (e.g. a categorical value). Defaults to None.
        file_max_size (Optional[str], optional): Desired size of export files. Must have suffix 'bytes', 'kb', 'mb', 'gb', 'tb'. (e.g. '500mb'). If None, no limit will be placed on file size. Defaults to None.
        file_stem_prefix (Optional[str], optional): A prefix put on every export file name. Defaults to None.
    """
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
    fo = fo or Files()
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
