import os
import re
from dataclasses import dataclass
from logging import Logger
from multiprocessing import JoinableQueue, Process
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import connectorx as cx
import pandas as pd
import sqlalchemy as sa
from pg_components import create_table
from sqlalchemy.orm.decl_api import DeclarativeMeta
from tqdm import tqdm

from quicklogs import get_logger

from .export_meta import create_export_meta
from .utils import engine_url, query_str, remove_engine_driver, split_schema_table


@dataclass
class DFFilter:
    # drop_columns (Optional[Sequence[str]], optional): _description_. Defaults to None.
    drop_columns: Optional[Sequence[str]] = None
    # column_type_converters (Optional[Dict[str, Any]], optional): _description_. Defaults to None.
    column_type_converters: Optional[Dict[str, Any]] = None
    # row_value_converters (Optional[Dict[str, Callable]], optional): _description_. Defaults to None.
    row_value_converters: Optional[Dict[str, Callable]] = None
    # callables that convert entire columns.
    column_converters: Optional[Dict[str, Callable]] = None
    drop_duplicates: Union[List[str], bool] = False

    def filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter columns before writing to file"""
        # drop columns if needed.
        if self.drop_columns:
            df.drop(self.drop_columns, axis=1, inplace=True)
        # convert column values.
        if self.column_type_converters:
            df = df.astype(self.column_type_converters)
        if self.row_value_converters:
            for col, val_cvt in self.row_value_converters.items():
                df[col] = df.apply(lambda row: val_cvt(row[col]), axis=1)
        if self.column_converters:
            for col, cvt in self.column_converters.items():
                df[col] = cvt(df[col])
        if self.drop_duplicates:
            df = df.drop_duplicates(
                subset=self.drop_duplicates
                if isinstance(self.drop_duplicates, (str, list))
                else None
            )
        return df


def create_pd_upsert_method(table: sa.Table, logger: Logger):
    """Create an upsert method that satisfies the Pandas to_sql API."""

    def upsert_rows(rows, conn):
        """Upsert rows into table."""
        # values_to_insert = [dict(zip(keys, data)) for data in data_iter]
        stmt = sa.dialects.postgresql.insert(table).values(rows)
        conn.execute(
            stmt.on_conflict_do_update(
                index_elements=table.primary_key.columns,
                set_={exc_k.key: exc_k for exc_k in stmt.excluded},
            )
        )

    def pd_upsert_method(_, conn, keys, data_iter):
        """
        Create upsert method that satisfied the Pandas to_sql API.
        """
        rows = list(data_iter)
        try:
            upsert_rows(rows, conn)
        except Exception as err:
            logger.error(
                "%s error upserting row batch: %s. Upserting individual rows...",
                type(err),
                err,
            )
            for row in rows:
                try:
                    upsert_rows(row, conn)
                except Exception as row_err:
                    logger.error(
                        "%s error upserting row: %s. Can not load row: %s.",
                        type(row_err),
                        row_err,
                        row,
                    )

    return pd_upsert_method


def copy_table_data(
    src: Union[sa.select, str, sa.Table],
    dst_table: sa.Table,
    src_engine: Union[str, sa.Engine],
    dst_engine: Union[str, sa.Engine],
    upsert: bool = True,
    transaction_rows: int = 1000,
    df_filter: Optional[DFFilter] = None,
    stream: bool = True,
    # --Applicable for n_workers > 1--
    slice_column: Optional[Union[str, sa.Column]] = None,
    partition_column: Optional[Union[str, sa.Column]] = None,
    task_max_size: str = "5gb",
    log_dir: Optional[Union[str, Path]] = None,
    finished_tasks_file: Optional[Union[str, Path]] = None,
    n_workers: int = 4,
):
    """Copy data from one table to another. Tables may be in different databases or on different servers.
    Note: If source database and destination database are in the same server, you can use a INSERT INTO ... (SELECT ...) statement as an alternative to this function.

    Args:
        src (Union[sa.select, str, sa.Table]): The table or query to copy from.
        dst_table (Union[str, sa.Table]): The table to copy to.
        src_engine (Union[str, sa.Engine]): URL or engine object for source database.
        dst_engine (Union[str, sa.Engine]): URL or engine object for destination database.
        log_dir (Union[str, Path]): Directory to save logs to.
        upsert (bool, optional): Use an upsert statement (instead of plain INSERT) when loading data. Defaults to True.
        transaction_rows (int, optional): Number of rows to load per transaction. Defaults to 1000.
        df_filter (Optional[DFFilter], optional): DataFrame filter operations to apply to data before loading to `dst_table`. Defaults to None.
        stream (bool, optional): Stream batches of data from `src` instead of loading everything into memory at once. Defaults to True.
        --Applicable for n_workers > 1--
        slice_column (Optional[Union[str, sa.Column]]): Slice table on this column to create parallelizable tasks. Defaults to None.
        partition_column (Optional[Union[str, sa.Column]]): Split table on unique values of this column to create parallelizable tasks. Defaults to None.
        task_max_size (Optional[str]): Max DataFrame size per task. Defaults to None.
        n_workers (int, optional): Number of parallel workers to use. Defaults to 4.
    """
    logger = _get_logger(log_dir)
    if str_src_eng_arg := isinstance(src_engine, str):
        src_engine = sa.create_engine(src_engine)
    if str_dst_eng_arg := isinstance(dst_engine, str):
        dst_engine = sa.create_engine(dst_engine)
    if isinstance(dst_table, (sa.Table, DeclarativeMeta)):
        with dst_engine.begin() as conn:
            create_table(conn, dst_table)
        dst_table = getattr(dst_table, "__table__", dst_table)
    elif isinstance(dst_table, str):
        schema, table_name = split_schema_table(dst_table)
        with dst_engine.begin() as conn:
            dst_table = sa.Table(
                table_name, sa.MetaData(schema=schema), autoload_with=conn
            )
    if str_dst_eng_arg:
        dst_engine.dispose()
    if (
        finished_tasks_file
        and (finished_tasks_file := Path(finished_tasks_file)).exists()
    ):
        finished_tasks = finished_tasks_file.read_text().splitlines()
    else:
        finished_tasks = set()
    logger.info("Preparing transfer tasks for %s.", src)
    src_query_q = JoinableQueue()
    logger.info(
        "Running transfer tasks with %i workers",
        n_workers,
    )
    kwargs = {
        "src_queue": src_query_q,
        "dst_table": dst_table,
        "src_engine": engine_url(src_engine),
        "dst_engine": engine_url(dst_engine),
        "upsert": upsert,
        "transaction_rows": transaction_rows,
        "df_filter": df_filter,
        "stream": stream,
        "log_dir": log_dir,
        "finished_tasks_file": finished_tasks_file,
    }
    procs = [
        Process(target=_copy_table_data_worker, kwargs=kwargs) for _ in range(n_workers)
    ]
    for p in tqdm(procs):
        p.start()

    if n_workers > 1 and (slice_column is not None or partition_column is not None):
        for meta in create_export_meta(
            table=src,
            engine=src_engine,
            save_locs=None,
            slice_column=slice_column,
            partition_column=partition_column,
            # TODO deal with this being compressed file size and not the in-memory size...
            file_max_size=task_max_size,
            file_stem_prefix=None,
            logger=logger,
        ):
            src_query, src_query_str = _get_src_query(meta.statement, src_engine)
            if src_query_str not in finished_tasks:
                src_query_q.put((src_query, src_query_str))
    else:
        src_query, src_query_str = _get_src_query(src, src_engine)
        if src_query_str not in finished_tasks:
            src_query_q.put((src_query, src_query_str))
    if str_src_eng_arg:
        src_engine.dispose()
    # wait for all tasks to finish.
    logger.info(
        "Finished creating tasks. Waiting for %i remaining tasks to finish.",
        src_query_q.qsize(),
    )
    src_query_q.join()


def _get_src_query(src, src_engine):
    if isinstance(src, sa.Select):
        # already in correct format.
        src_query = src
    elif isinstance(src, (sa.Table, DeclarativeMeta)):
        # select entire table.
        src_query = sa.select(src)
    if not isinstance(src, str):
        raise ValueError(f"Invalid src type ({type(src)}): {src}")
    if re.match(r"^[\w.]+$", src):
        # string is table name. select entire table.
        src_query = sa.text(f"SELECT * FROM {src}")
    else:
        # make query string object.
        src_query = sa.text(src)
    src_query_str = query_str(src_query, src_engine)
    return src_query, src_query_str


def _get_logger(log_dir=None):
    return get_logger(f"cp-tbl-data-{os.getpid()}", file_dir=log_dir)


def _copy_table_data_worker(
    src_queue,
    dst_table,
    src_engine,
    dst_engine,
    upsert,
    transaction_rows,
    df_filter,
    stream,
    log_dir,
    finished_tasks_file=None,
):
    logger = _get_logger(log_dir)
    logger.info("Starting transfer worker.")
    if isinstance(src_engine, str):
        src_engine = sa.create_engine(src_engine)
    if isinstance(dst_engine, str):
        dst_engine = sa.create_engine(dst_engine)

    def to_sql(dst_con, df, transaction_rows=None):
        df.to_sql(
            name=dst_table.name,
            con=dst_con,
            schema=dst_table.schema,
            index=False,
            if_exists="append",
            chunksize=transaction_rows,
            method=create_pd_upsert_method(dst_table, logger) if upsert else "multi",
        )

    while True:
        logger.info("Waiting for transfer task.")
        src_query, src_query_str = src_queue.get()
        logger.info(
            "Starting next transfer task: %s. %i tasks remaining",
            src_query_str,
            src_queue.qsize(),
        )
        if stream:
            with src_engine.connect().execution_options(
                stream_results=True
            ) as src_conn:
                for df in pd.read_sql_query(
                    src_query, src_conn, chunksize=transaction_rows
                ):
                    try:
                        if df_filter:
                            df = df_filter.filter_df(df)
                        with dst_engine.begin() as dst_con:
                            to_sql(dst_con, df)
                    except Exception as e:
                        logger.error(
                            "%s error loading chunk: %s. Query: %s",
                            type(e),
                            e,
                            src_query_str,
                        )
        else:
            src_query = query_str(src_query, src_engine)
            df = cx.read_sql(
                remove_engine_driver(engine_url(src_engine)), query=src_query
            )
            if df_filter:
                df = df_filter.filter_df(df)
            logger.info("Loading DataFrame %s from %s", df.shape, src_query)
            to_sql(dst_engine, df, transaction_rows)
        logger.info("Finished transfer task: %s", src_query_str)
        if finished_tasks_file is not None:
            with finished_tasks_file.open(mode="a+") as fo:
                fo.write(f"{src_query_str}\n")
        src_queue.task_done()
