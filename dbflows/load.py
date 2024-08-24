import asyncio
from logging import Logger
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import sqlalchemy as sa
import ujson as json
from cytoolz.itertoolz import groupby, partition_all
from quicklogs import get_logger
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm.decl_api import DeclarativeMeta

from .tables import create_tables
from .utils import compile_statement, get_connection_pool
from .utils import schema_table as get_schema_table
from .utils import to_table


async def load_rows(
    table: Union[sa.Table, DeclarativeMeta],
    pg_url: str,
    rows: List[Dict[str, Any]],
    *args,
    **kwargs,
):
    loader = await PgLoader.create(table=table, pg_url=pg_url, *args, **kwargs)
    await loader.load_rows(rows=rows)


async def load_row(
    table: Union[sa.Table, DeclarativeMeta],
    pg_url: str,
    row: Dict[str, Any],
    *args,
    **kwargs,
):
    loader = await PgLoader.create(table=table, pg_url=pg_url, *args, **kwargs)
    await loader.load_row(row=row)


class PgLoader:
    @classmethod
    async def create(
        cls,
        table: Union[sa.Table, DeclarativeMeta],
        pg_url: str,
        on_duplicate_key_update: Optional[Union[bool, List[str]]] = None,
        key_columns: Optional[List[str]] = None,
        row_batch_size: int = 1500,
        duplicate_key_rows_keep: Optional[Literal["first", "last"]] = None,
        remove_rows_missing_key: bool = False,
        column_name_map: Optional[Dict[str, str]] = None,
        column_names_converter: Callable[[str], str] = None,
        column_name_converters: Optional[Dict[str, Callable[[str], str]]] = None,
        value_map: Optional[Dict[Any, Any]] = None,
        column_values_converter: Optional[Callable[[Any], Any]] = None,
        column_value_converters: Optional[Dict[str, Callable[[Any], Any]]] = None,
        group_by_columns_present: bool = True,
        max_conn: int = 10,
        logger: Optional[Logger] = None,
    ) -> None:
        """Load data rows to a postgresql database.
        Name converters are the first thing applied, so arguments mapping from name should use the name in converted form.

        Args:
            table (Union[sa.Table, DeclarativeMeta]): The SQLAlchemy table or entity corresponding to the database table that rows will be loaded to.
            dsn (str): The PostgreSQL dsn connection string to the database.
            on_duplicate_key_update (Union[bool, List[str]], optional): List of columns that should be updated when key columns exists, or True for all columns, False for no columns, None if duplicates should not be checked (i.e. a normal INSERT). Defaults to None.
            key_columns (Optional[List[str]], optional): List of key columns that should be used for on_duplicate_key_update. Default to primary key.
            row_batch_size (int): Number of rows to load per statement. Defaults to 1500.
            column_name_map (Optional[Dict[str, str]], optional): Map column name to desired column name. Defaults to None.
            column_names_converter (Callable[[str], str], optional): A formatting function to apply to every name. Defaults to None.
            column_name_converters (Optional[Dict[str, Callable[[str], str]]], optional): A formatting function to apply to every name. Defaults to None.
            duplicate_key_rows_keep (bool, optional): Remove duplicates from upsert batches. Last instance of row will be kept. Defaults to False.
            remove_rows_missing_key (bool, optional): _description_. Defaults to True.
            column_to_value (Optional[Dict[str, Any]], optional): Map column name to desired column value. Defaults to None.
            column_value_converters (Optional[Dict[str, Callable[[Any], Any]]], optional): Map column name to column value conversion function. Defaults to None.
            column_values_converter (Optional[Callable[[Any], Any]], optional): A conversion function to apply to every value. Defaults to None.
            value_map (Optional[Dict[Any, Any]], optional): Map value to desired value. Defaults to None.
            group_by_columns_present (bool, optional): Group rows by columns present and execute upsert statement for each group. Defaults to True.
            max_conn (int, optional): Maximum number of connections to use in connection pool. Defaults to 10.
            logger (Optional[Logger], optional): Logger to use. Defaults to None.
        """
        self = cls()
        self.table = to_table(table)
        self.schema_table = get_schema_table(self.table)
        self.pool = await get_connection_pool(pg_url, max_conn=max_conn)
        self.row_batch_size = row_batch_size
        self.on_duplicate_key_update = on_duplicate_key_update
        self.group_by_columns_present = group_by_columns_present
        self.key_columns = key_columns or {
            c.name for c in self.table.primary_key.columns
        }
        self.logger = logger or get_logger(f"{self.table.name}-loader")
        self.duplicate_key_rows_keep = duplicate_key_rows_keep
        if not self.key_columns:
            # not applicable.
            remove_rows_missing_key = False
            self.on_duplicate_key_update = None
            self.duplicate_key_rows_keep = None

        self._filters = []
        # do column name filtering first, so other functions will use the filtered names.
        if column_name_map or column_names_converter or column_name_converters:
            self._filters.append(
                create_column_name_converter(
                    column_name_map, column_names_converter, column_name_converters
                )
            )

        table_column_names = {str(c) for c in self.table.columns.keys()}

        def apply_remove_unwanted_columns(
            rows: List[Dict[str, Any]]
        ) -> List[Dict[str, Any]]:
            rows = [{c: row[c] for c in table_column_names if c in row} for row in rows]
            return [row for row in rows if len(row)]

        self._filters.append(apply_remove_unwanted_columns)

        if remove_rows_missing_key:

            def apply_remove_rows_missing_key(rows: List[Dict[str, Any]]):
                return [row for row in rows if all(c in row for c in self.key_columns)]

            self._filters.append(apply_remove_rows_missing_key)

        if self.duplicate_key_rows_keep:
            self._filters.append(self._apply_duplicate_key_rows_keep)

        if value_map:

            def apply_value_map(rows):
                """Set values to desired alternative value."""
                filtered = []
                for row in rows:
                    for col, val in row.items():
                        try:
                            row[col] = value_map.get(val, val)
                        except TypeError:
                            pass
                    filtered.append(row)
                return filtered

            self._filters.append(apply_value_map)

        if column_values_converter:

            def apply_value_converter(rows):
                """Apply value converter function to all values in row."""
                for row in rows:
                    for col, val in row.items():
                        row[col] = column_values_converter(val)
                return rows

            self._filters.append(apply_value_converter)

        # check for JSON columns.
        json_cols = [
            c.name for c in self.table.columns.values() if isinstance(c.type, JSON)
        ]
        if json_cols:
            if column_value_converters is None:
                column_value_converters = {}
            for col in json_cols:
                if col not in column_value_converters:
                    column_value_converters[col] = lambda v: (
                        json.dumps(v) if v else None
                    )

        if column_value_converters:

            def apply_column_value_converters(rows):
                """For specified columns, apply specified column value converter functions."""
                for row in rows:
                    for col, val_cvt in column_value_converters.items():
                        if col in row:
                            row[col] = val_cvt(row[col])
                return rows

            self._filters.append(apply_column_value_converters)

        # determine what should be updated when there is an existing primary key.
        if self.on_duplicate_key_update == True:
            # update all columns that aren't primary key.
            self.on_duplicate_key_update = [
                col_name
                for col_name, col in self.table.columns.items()
                if not col.primary_key
            ]

        key_cols = ",".join([f'"{c}"' for c in self.key_columns])
        insert_statement = f"INSERT INTO {self.schema_table} ({{}}) VALUES {{}}"
        if self.on_duplicate_key_update:
            # update provided columns.
            update_cols = ",".join(
                [f"{col}=EXCLUDED.{col}" for col in self.on_duplicate_key_update]
            )
            self._statement_template = f"{insert_statement} ON CONFLICT ({key_cols}) DO UPDATE SET {update_cols}"
        elif self.on_duplicate_key_update == False:
            self._statement_template = (
                f"{insert_statement} ON CONFLICT ({key_cols}) DO NOTHING"
            )
        elif not self.on_duplicate_key_update:
            self._statement_template = insert_statement
        else:
            raise ValueError(
                f"Invalid argument for on_duplicate_key_update: {self.on_duplicate_key_update}"
            )
        # create table if it doesn't already exist.
        async with self.pool.acquire() as conn:
            await create_tables(conn=conn, create_from=self.table)
        return self

    async def load_row(self, row: Dict[str, Any] = None, **kwargs):
        if row:
            row.update(kwargs)
        else:
            row = kwargs
        row = self.filter_rows([row])
        if not row:
            return
        await self._load(row)

    async def load_rows(self, rows: List[Dict[str, Any]]):
        """Load rows to the database.

        Args:
            rows (List[Dict[str, Any]]): Rows to that should be loaded.
        """
        rows = self.filter_rows(rows)
        if not rows:
            return
        # upsert all batches.
        self.logger.info(
            "Loading %i rows to %s.",
            len(rows),
            self.schema_table,
        )
        if self.group_by_columns_present:
            row_groups = groupby_columns(rows)
            # split rows into smaller batches if there are too many to insert at once.
            tasks = [
                asyncio.create_task(self._load(batch))
                for rows in row_groups
                for batch in partition_all(self.row_batch_size, rows)
            ]
        else:
            tasks = [
                asyncio.create_task(self._load(batch))
                for batch in partition_all(self.row_batch_size, rows)
            ]
        for task in asyncio.as_completed(tasks):
            await task

    def filter_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply all filter functions to rows.

        Args:
            rows (List[Dict[str, Any]]): The rows to filter in addition to rows buffered with `add` and `extend`. Defaults to None.

        Returns:
            List[Dict[str, Any]]: The filtered rows.
        """
        for filter_func in self._filters:
            if not (rows := filter_func(rows)):
                self.logger.info(
                    "No rows remain after applying filter function: %s",
                    filter_func.__name__,
                )
                break
        return rows

    async def fetch(self, query: sa.Select) -> List[Any]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(compile_statement(query))

    async def fetchrow(self, query: sa.Select) -> List[Any]:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(compile_statement(query))

    async def fetchval(self, query: sa.Select) -> List[Any]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(compile_statement(query))

    async def _load(self, rows: List[List[Any]]):
        # rows should all contain same columns.
        columns = list(rows[0].keys())
        n_cols = len(columns)
        start = 1
        row_place_holders, values = [], []
        for row in rows:
            end = start + n_cols
            row_place_holders.append(
                f"({','.join([f'${i}' for i in range(start, end)])})"
            )
            start = end
            for col in columns:
                values.append(row.get(col))
        row_place_holders = ",".join(row_place_holders)
        statement = self._statement_template.format(
            ",".join([f'"{c}"' for c in columns]), row_place_holders
        )
        async with self.pool.acquire() as conn:
            await conn.execute(statement, *values)

    def _apply_duplicate_key_rows_keep(
        self,
        rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Remove rows that repeat a primary key.
        Multiple row in the same upsert statement can not have the same primary key.

        Args:
            rows (List[Dict[str, Any]]): Data row, possibly containing duplicates.

        Returns:
            List[Dict[str, Any]]: Duplicate-free data rows.
        """
        row_count = len(rows)
        if self.duplicate_key_rows_keep == "first":
            rows = reversed(rows)
        unique_key_rows = {
            tuple([row[c] for c in self.key_columns]): row for row in rows
        }
        unique_key_rows = list(unique_key_rows.values())
        unique_count = len(unique_key_rows)
        if unique_count < row_count:
            self.logger.warning(
                "%i/%i rows had a duplicate key and will not be loaded.",
                row_count - unique_count,
                row_count,
            )
        return unique_key_rows


def create_column_name_converter(
    column_name_map, all_names_converter, column_name_converters
) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Filter columns and convert to match database column names.

    Args:
        column_name_map (Optional[Dict[str, str]], optional): Map column name to desired column name. Defaults to None.
        all_names_converter (Callable, optional): A formatting function to apply to every name. Defaults to None.
        column_name_converters (Optional[Dict[str, Callable]], optional): Map column name to formatting function. Defaults to None.

    Returns:
        Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]: The filter function.
    """

    converters = []
    if all_names_converter:
        converters.append(all_names_converter)
    if column_name_converters:
        converters.append(
            lambda name: (
                column_name_converters[name](name)
                if name in column_name_converters
                else name
            )
        )
    if column_name_map:
        converters.append(lambda name: column_name_map.get(name, name))
    if not converters:
        return

    converted_column_names: Dict[str, str] = {}

    def _convert_name(name: str) -> str:
        """Convert `name` to a table column name.

        Args:
            name (str): The Name that should be converted.

        Returns:
            str: The converted name.
        """
        orig_name = name
        for func in converters:
            name = func(name)
        converted_column_names[orig_name] = name
        return name

    def _filter_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter unwanted columns from `row` and convert names to database column names.

        Args:
            row (Dict[str, Any]): The row to be filtered.

        Returns:
            Dict[str, Any]: The filtered row.
        """
        rows = [
            {
                converted_column_names.get(c) or _convert_name(c): v
                for c, v in row.items()
            }
            for row in rows
        ]
        return rows

    return _filter_rows


def groupby_columns(rows: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Group rows by column names present.
    We can not have rows with different columns in the same statement.

    Args:
        rows (List[Dict[str,Any]]): The rows to group.

    Returns:
        List[List[Dict[str, Any]]]: The grouped rows.
    """
    return groupby(lambda r: tuple(r.keys()), rows).values()
