import asyncio
import os
from logging import Logger
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import asyncpg
import sqlalchemy as sa
from asyncpg.exceptions import CardinalityViolationError
from cytoolz.itertoolz import groupby, partition_all
from quicklogs import get_logger
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.exc import CompileError
from sqlalchemy.orm.decl_api import DeclarativeMeta

from .utils import compile_sa_statement, to_table


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
    rows: Dict[str, Any],
    *args,
    **kwargs,
):
    loader = await PgLoader.create(table=table, pg_url=pg_url, *args, **kwargs)
    await loader.load_row(rows=rows)


class PgLoader:
    @classmethod
    async def create(
        cls,
        table: Union[sa.Table, DeclarativeMeta],
        pg_url: str,
        on_duplicate_key_update: Optional[Union[bool, List[str]]] = True,
        row_batch_size: int = 1500,
        duplicate_key_rows_keep: Optional[Literal["first", "last"]] = None,
        remove_rows_missing_key: bool = False,
        column_name_map: Optional[Dict[str, str]] = None,
        column_names_converter: Callable[[str], str] = None,
        column_name_converters: Optional[Dict[str, Callable[[str], str]]] = None,
        value_map: Optional[Dict[Any, Any]] = None,
        column_values_converter: Optional[Callable[[Any], Any]] = None,
        column_value_converters: Optional[Dict[str, Callable[[Any], Any]]] = None,
        group_by_columns_present: bool = False,
        max_conn: int = int(os.cpu_count() * 0.8),
        logger: Optional[Logger] = None,
    ) -> None:
        """Load data rows to a postgresql database.
        Name converters are the first thing applied, so arguments mapping from name should use the name in converted form.

        Args:
            table (Union[sa.Table, DeclarativeMeta]): The SQLAlchemy table or entity corresponding to the database table that rows will be loaded to.
            dsn (str): The PostgreSQL dsn connection string to the database.
            on_duplicate_key_update (Union[bool, List[str]], optional): List of columns that should be updated when primary key exists, or True for all columns, False for no columns, None if duplicates should not be checked (i.e. a normal INSERT). Defaults to True.
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
            max_conn (int, optional): Maximum number of connections to use. Defaults to int(os.cpu_count() * 0.8).
            logger (Optional[Logger], optional): Logger to use. Defaults to None.
        """
        self = cls()
        self.table = to_table(table)
        self.pool = await asyncpg.create_pool(dsn=pg_url)
        self.row_batch_size = row_batch_size
        self.on_duplicate_key_update = on_duplicate_key_update
        self.group_by_columns_present = group_by_columns_present
        self.max_conn = max_conn
        self.logger = logger or get_logger(f"{self.table.name}-loader")
        self.duplicate_key_rows_keep = duplicate_key_rows_keep
        self._primary_key_column_names = _primary_key_column_names = {
            c.name for c in self.table.primary_key.columns
        }
        if not _primary_key_column_names:
            # not applicable because there are no keys.
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
                return [
                    row
                    for row in rows
                    if all(c in row for c in _primary_key_column_names)
                ]

            self._filters.append(apply_remove_rows_missing_key)

        if self.duplicate_key_rows_keep:
            self._filters.append(self._apply_duplicate_key_rows_keep)

        if value_map:

            def apply_value_map(rows):
                """Set values to desired alternative value."""
                return [
                    {k: value_map.get(v, v) for k, v in row.items()} for row in rows
                ]

            self._filters.append(apply_value_map)

        if column_values_converter:

            def apply_value_converter(rows):
                """Apply value converter function to all values in row."""
                for row in rows:
                    for col, val in row.items():
                        row[col] = column_values_converter(val)
                return rows

            self._filters.append(apply_value_converter)

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
        if self.on_duplicate_key_update:
            # update provided columns.
            self._build_statement = self._upsert_update_statement
        elif self.on_duplicate_key_update == False:
            self._build_statement = self._upsert_ignore_statement
        elif not self.on_duplicate_key_update:
            self._build_statement = self._insert_statement
        else:
            raise ValueError(
                f"Invalid argument for on_duplicate_key_update: {self.on_duplicate_key_update}"
            )
        # create table if it doesn't already exist.
        # await async_create_table(create_from=self.table, engine=self.pool)
        return self

    async def load_row(self, row: Dict[str, Any]):
        await self._load(row)

    async def load_rows(self, rows: List[Dict[str, Any]]):
        """Load rows to the database.

        Args:
            rows (List[Dict[str, Any]]): Rows to that should be loaded.
        """
        rows = self.filter_rows(rows)
        if not rows:
            return []
        # upsert all batches.
        self.logger.info(
            "Loading %i rows to the database.",
            len(rows),
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
            self.logger.info("Finished loading batch.")

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
            return await conn.fetch(compile_sa_statement(query))

    async def fetchrow(self, query: sa.Select) -> List[Any]:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(compile_sa_statement(query))

    async def fetchval(self, query: sa.Select) -> List[Any]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(compile_sa_statement(query))

    async def _load(self, rows):
        async with self.pool.acquire() as conn:
            try:
                statement = compile_sa_statement(self._build_statement(rows))
                await conn.execute(statement)
            except CardinalityViolationError as e1:
                if (self.duplicate_key_rows_keep is None) and (
                    "command cannot affect row a second time" in e1._message()
                ):
                    return await self._load(self._apply_duplicate_key_rows_keep(rows))
                else:
                    raise e1
            except CompileError as ce:
                if (
                    not self.group_by_columns_present
                    and "is explicitly rendered as a boundparameter in the VALUES clause"
                    in ce._message()
                ):
                    return await asyncio.gather(
                        *[
                            asyncio.create_task(self._load(rows))
                            for rows in groupby_columns(rows)
                        ]
                    )
                else:
                    raise ce

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
        if self.duplicate_key_rows_keep == "first":
            rows = reversed(rows)
        unique_key_rows = {
            tuple([row[c] for c in self._primary_key_column_names]): row for row in rows
        }
        unique_key_rows = list(unique_key_rows.values())
        if (unique_count := len(unique_key_rows)) < (row_count := len(rows)):
            self.logger.warning(
                "%i/%i rows had a duplicate primary key and will not be loaded.",
                row_count - unique_count,
                row_count,
            )
        return unique_key_rows

    def _insert_statement(self, rows: List[Dict[str, Any]]) -> Insert:
        """Construct a statement to insert `rows`.

        Args:
            rows (List[Dict[str,Any]]): The rows that will be loaded.

        Returns:
            Insert: An insert statement.
        """
        return postgresql.insert(self.table).values(rows)

    def _upsert_update_statement(self, rows: List[Dict[str, Any]]) -> Insert:
        """Construct a statement to load `rows`.

        Args:
            rows (List[Dict[str,Any]]): The rows that will be loaded.

        Returns:
            Insert: An upsert statement.
        """

        # check column of first row (all rows should have same columns)
        sample_row = rows[0]
        on_duplicate_key_update = [
            c for c in self.on_duplicate_key_update if c in sample_row
        ]
        if len(on_duplicate_key_update):
            statement = postgresql.insert(self.table).values(rows)
            return statement.on_conflict_do_update(
                index_elements=self._primary_key_column_names,
                set_={k: statement.excluded[k] for k in on_duplicate_key_update},
            )
        return self._upsert_ignore_statement(rows)

    def _upsert_ignore_statement(self, rows: List[Dict[str, Any]]) -> Insert:
        """Construct a statement to load `rows`.

        Args:
            rows (List[Dict[str,Any]]): The rows that will be loaded.

        Returns:
            Insert: An upsert statement.
        """
        return (
            postgresql.insert(self.table)
            .values(rows)
            .on_conflict_do_nothing(index_elements=self._primary_key_column_names)
        )


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
