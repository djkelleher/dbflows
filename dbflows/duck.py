import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import current_thread
from typing import List, Optional, Sequence

import duckdb
import pandas as pd
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
    statements: Sequence[str],
    conn: DuckDBPyConnection,
    n_threads: Optional[int] = None,
) -> List[pd.DataFrame]:
    if isinstance(statements, (list, tuple, set)) and len(statements) == 1:
        statements = statements[0]
    if isinstance(statements, str):
        if conn:
            return [conn.execute(statements).df()]
        with duckdb.connect() as conn:
            return [conn.execute(statements[0]).df()]
    stmt_q = Queue()
    for stmt in statements:
        stmt_q.put(stmt)
    n_stmt = stmt_q.qsize()
    n_threads = n_threads or min(n_stmt, int(os.cpu_count() * 0.7))
    logger.info("Executing %i statements with %i threads", n_stmt, n_threads)
    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futures = [
            pool.submit(execute_statement_queue, conn, stmt_q) for _ in range(n_threads)
        ]
    results = []
    for future in as_completed(futures):
        result = future.result()
        results.extend(result)
    logger.info("Finished executing %i statements with %i threads", n_stmt, n_threads)
    return results


def execute_statement_queue(conn: DuckDBPyConnection, statement_q: Queue):
    # Create a DuckDB connection specifically for this thread
    local_conn = conn.cursor()
    thread_name = current_thread().name
    results = []
    while not statement_q.empty():
        try:
            statement = statement_q.get(timeout=1)
        except TimeoutError:
            break
        try:
            results.append(local_conn.execute(statement).df())
        except Exception as err:
            logger.exception(
                "Thread %s %s error processing statement (%s): %s",
                thread_name,
                type(err),
                statement,
                err,
            )
    logger.info("Thread %s finished.", thread_name)
    return results


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
