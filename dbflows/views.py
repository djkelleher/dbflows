from typing import Dict, List, Optional, Sequence, Union

import sqlalchemy as sa
from sqlalchemy import func as fn
from sqlalchemy.engine import Engine

from dbflows.utils import compile_statement, logger, schema_table

from .utils import DbObj, query_kwargs

pg_views_meta_table = sa.Table(
    "views",
    sa.MetaData(schema="information_schema"),
    sa.Column("table_schema", sa.Text),
    sa.Column("table_name", sa.Text),
    sa.Column("view_definition", sa.Text),
)


class View(DbObj):
    """A view table."""

    def __init__(
        self,
        table: sa.Table,
        query: sa.Select,
        comment: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """
        Args:
            table (sa.Table): table with columns returned by `query`
            query (sa.Select): Query used to select data for the view.
            comment (Optional[str], optional): Comment on the view. Defaults to None.
            replace_existing (Optional[bool], optional): Recreate the view if it already exists.
        """
        self.table = table
        self.query = query
        self.comment = comment
        self.replace_existing = replace_existing

    @property
    def name(self) -> str:
        return schema_table(self.table)

    def create(self, engine: Engine):
        """Create the view in the database."""
        if not self.replace_existing:
            # don't replace a view that already exists.
            with engine.begin() as conn:
                exists = execute_sql(
                    sa.select(
                        sa.exists(sa.text("1"))
                        .select_from(pg_views_meta_table)
                        .where(
                            pg_views_meta_table.c.table_schema == self.table.schema,
                            pg_views_meta_table.c.table_name == self.table.name,
                        )
                    ),
                    conn,
                ).scalar()
            if exists:
                logger.info(
                    "View (%s) already exists. Not recreating.",
                    self.table.name,
                )
                return

        statement = (
            f"CREATE OR REPLACE VIEW {self.name} AS {compile_statement(self.query)}"
        )
        execute_sql(statement, engine)
        if self.comment:
            execute_sql(
                f"COMMENT ON VIEW {self.name} IS '{self.comment}';",
                engine,
            )

    @staticmethod
    def list_all(
        engine: Engine, schema: Optional[str] = None, like_pattern: Optional[str] = None
    ) -> List[str]:
        query = sa.select(
            fn.concat(
                pg_views_meta_table.c.table_schema,
                ".",
                pg_views_meta_table.c.table_name,
            )
        ).select_from(pg_views_meta_table)
        if schema:
            query = query.where(pg_views_meta_table.c.table_schema == schema)
        if like_pattern:
            query = query.where(pg_views_meta_table.c.table_name.like(like_pattern))
        with engine.begin() as conn:
            view_names = list(execute_sql(query, conn).scalars())
        return view_names

    def drop(self, engine: Engine, cascade: bool = True):
        """Drop the view from the database."""
        self.drop_view(engine, self.name, cascade=cascade)

    @staticmethod
    def drop_view(engine: Engine, schema_table: str, cascade: bool = True):
        logger.info("Dropping view %s", schema_table)
        statement = [f"DROP VIEW IF EXISTS {schema_table}"]
        if cascade:
            statement.append("CASCADE")
        execute_sql(" ".join(statement), engine)


class MaterializedView(View):
    """A materialized view table."""

    def __init__(
        self,
        create_with_no_data: bool = True,
        storage_params: Optional[Union[Sequence[str], Dict[str, str]]] = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Args:
            create_with_no_data (bool, optional): Create the view without loading data. Defaults to True.
            storage_params (Optional[Union[Sequence[str], Dict[str, str]]], optional): postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS. Defaults to None.
        """
        super().__init__(*args, **kwargs)
        self.storage_params = storage_params
        self.create_with_no_data = create_with_no_data

    def create(self, engine: Engine):
        """Create the view in the database."""
        if self.replace_existing:
            # can't replace a materialized view, so drop it first.
            self.drop(engine)
        statement = ["CREATE MATERIALIZED VIEW"]
        if self.replace_existing is None:
            statement.append("IF NOT EXISTS")
        statement.append(self.name)
        if isinstance(self.storage_params, dict):
            self.storage_params = query_kwargs(self.storage_params)
        elif isinstance(self.storage_params, (list, tuple)):
            self.storage_params = ",".join(self.storage_params)
        if self.storage_params:
            statement.append(f"WITH ({self.storage_params})")
        statement.append(f"AS {compile_statement(self.query)}")
        if self.create_with_no_data:
            statement.append("WITH NO DATA")
        execute_sql(" ".join(statement), engine)
        if self.comment:
            execute_sql(
                f"COMMENT ON MATERIALIZED VIEW {self.name} IS '{self.comment}';",
                engine,
            )

    @staticmethod
    def list_all(
        engine: Engine, schema: Optional[str] = None, like_pattern: Optional[str] = None
    ) -> List[str]:
        # TODO sa table
        schemaname_col = sa.column("schemaname")
        MaterializedViewname_col = sa.column("MaterializedViewname")
        query = sa.select(
            fn.concat(schemaname_col, ".", MaterializedViewname_col)
        ).select_from(sa.text("pg_MaterializedViews"))
        if schema:
            query = query.where(schemaname_col == schema)
        if like_pattern:
            query = query.where(MaterializedViewname_col.like(like_pattern))
        with engine.begin() as conn:
            view_names = list(execute_sql(query, conn).scalars())
        return view_names

    def drop(self, engine: Engine, cascade: bool = True):
        """Drop the view from the database."""
        self.drop_view(engine, self.name, cascade=cascade)

    @staticmethod
    def drop_view(engine: Engine, schema_table: str, cascade: bool = True):
        logger.info("Dropping materialized view %s", schema_table)
        statement = [f"DROP MATERIALIZED VIEW IF EXISTS {schema_table}"]
        if cascade:
            statement.append("CASCADE")
        execute_sql(" ".join(statement), engine)
