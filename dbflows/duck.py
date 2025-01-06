import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Queue
from threading import current_thread
from typing import List, Optional, Sequence

import duckdb
from duckdb import DuckDBPyConnection

from .utils import logger


def create_table(conn: DuckDBPyConnection, schema_table: str, columns: str):
    if schema := re.match(r'^([^."]+)\.', schema_table):
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema.group(1)}")
    conn.execute(f"CREATE TABLE IF NOT EXISTS {schema_table}({columns})")


def get_table_names(conn, schema: Optional[str] = None) -> List[str]:
    query = "SELECT name FROM (SHOW ALL TABLES)"
    if schema:
        query += f" WHERE schema = '{schema}'"
    return conn.execute(query).df()["name"].to_list()


def execute_parallel(
    statements: Sequence[str] | Queue,
    conn: DuckDBPyConnection,
    n_threads: Optional[int] = None,
):
    if isinstance(statements, (list, tuple, set)):
        stmt_q = Queue()
        for stmt in statements:
            stmt_q.put(stmt)
    else:
        stmt_q = statements

    n_stmt = stmt_q.qsize()
    n_threads = n_threads or min(n_stmt, int(os.cpu_count() * 0.7))
    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futures = [
            pool.submit(execute_statement_queue, conn, stmt_q) for _ in range(n_threads)
        ]
    for future in as_completed(futures):
        result = future.result()
        logger.info("DuckDB thread result: %s", result)
    logger.info("Finished executing %i statements", n_stmt)


def execute_statement_queue(conn: DuckDBPyConnection, statement_q: Queue):
    # Create a DuckDB connection specifically for this thread
    local_conn = conn.cursor()
    thread_name = current_thread().name
    while not statement_q.empty():
        try:
            statement, table_name = statement_q.get(timeout=1)
        except TimeoutError:
            break
        logger.info("Thread %s processing table %s", thread_name, table_name)
        try:
            local_conn.execute(statement)
            logger.info("Thread %s processing table %s", thread_name, table_name)
        except Exception as err:
            logger.exception(
                "Thread %s %s error processing table %s: %s",
                thread_name,
                type(err),
                table_name,
                err,
            )
    logger.info("Thread %s finished.", thread_name)


def mount_pg_db(
    pg_url: str, conn: Optional[DuckDBPyConnection] = None
) -> str:
    """Mount a Postgresql database to DuckDB (so it can be queried by DuckDB)."""
    conn = conn or duckdb

    pg_db_name = pg_url.split("/")[-1]

    # Remove the 'postgresql+...' driver from a SQLAlchemy URL.
    pg_url = re.sub(r"^postgresql\+\w+:", "postgresql:", pg_url)
    try:
        conn.execute(f"ATTACH '{pg_url}' AS {pg_db_name} (TYPE POSTGRES)")
    except duckdb.BinderException as err:
        if f'database with name "{pg_db_name}" already exists' in str(err):
            logger.warning("Database is already attached: %s", pg_db_name)
        else:
            raise
    return pg_db_name
