import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import count
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, Sequence, Union

import sqlalchemy as sa
from fileflows import Files
from sqlalchemy.engine import Engine
from fileflows.s3 import is_s3_path

from .meta import ExportMeta, create_export_meta
from .utils import copy_to_csv, logger, schema_table


def export_table(
    table: Union[str, sa.Table],
    engine: Union[str, Engine],
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    slice_column: Optional[Union[str, sa.Column]] = None,
    partition_column: Optional[Union[str, sa.Column]] = None,
    file_max_size: Optional[str] = None,
    file_stem_prefix: Optional[str] = None,
    n_workers: Optional[int] = None,
    fo: Optional[Files] = None,
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
        fo (Optional[Files], optional): File operations interface for s3/local disk.
    """
    exports_meta = create_export_meta(
        table=table,
        engine=engine,
        save_locs=save_locs,
        slice_column=slice_column,
        partition_column=partition_column,
        file_max_size=file_max_size,
        file_stem_prefix=file_stem_prefix,
        fo=fo,
    )
    # run all export tasks.
    export_all(
        engine=engine,
        exports=exports_meta,
        append=slice_column is not None,
        save_locs=save_locs,
        n_workers=n_workers,
        fo=fo,
    )


def export_hypertable_chunks(
    engine: Union[str, Engine],
    table: Union[sa.Table, str],
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    n_workers: Optional[int] = None,
    fo: Optional[Files] = None,
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
        table = schema_table(table)
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
    export_all(
        engine,
        exports,
        append=False,
        save_locs=save_locs,
        n_workers=n_workers,
        fo=fo,
    )


def export(
    meta: ExportMeta,
    engine: Engine,
    append: bool,
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    fo: Optional[Files] = None,
):
    """Export data as specified in `meta`."""
    fo = fo or Files()
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
    elif is_s3_path(primary_save_loc):
        save_path = f"{TemporaryDirectory().name}/{created_file_name}"
    else:
        # primary save location is a local file, so save directly to primary location.
        save_path = prim_loc_file
    copy_to_csv(
        to_copy=meta.statement, engine=engine, save_path=save_path, append=append
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
    # if primary save location is s3, we need to move the file to s3.
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


def export_all(
    engine: Engine,
    exports: Sequence[ExportMeta],
    append: bool,
    save_locs: Union[Union[Path, str], Sequence[Union[Path, str]]],
    n_workers: Optional[int] = None,
    fo: Optional[Files] = None,
):
    """Export a data file for each `ExportMeta` in `exports`."""
    if n_workers is None:
        n_workers = int(os.cpu_count() * 0.8)
    logger.info("Starting exports with %i workers", n_workers)

    with ThreadPoolExecutor(max_workers=n_workers) as p:
        tasks = [
            p.submit(
                export,
                meta=meta,
                engine=engine,
                append=append,
                save_locs=save_locs,
                fo=fo,
            )
            for meta in exports
        ]
        n_tasks = len(tasks)
        logger.info("Waiting for %i export tasks to finish...", n_tasks)
        n_complete = count(1)
        for task in as_completed(tasks):
            task.result()
            logger.info("Finished export %i/%i", next(n_complete), n_tasks)
