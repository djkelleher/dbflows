from typing import Any, Dict, List, Optional, Union

import sqlalchemy as sa
from sqlalchemy import func as fn
from sqlalchemy.engine import Compiled, Engine

from dbgress.utils import execute_sql, logger

from .base import DbObj
from .schedule import SchedJob

routines_table = sa.Table(
    "routines",
    sa.MetaData(schema="information_schema"),
    sa.Column("routine_schema", sa.Text),
    sa.Column("routine_name", sa.Text),
    sa.Column("routine_type", sa.Text),
)


class Procedure(DbObj):
    """A stored procedure."""

    def __init__(
        self,
        name: str,
        statement: Optional[Any] = None,
        comment: Optional[str] = None,
        schedule: Optional[SchedJob] = None,
    ) -> None:
        """
        Args:
            name (str): Schema qualified name of the procedure.
            statement (Any): SQL statement list of statements to execute in the procedure.
            comment (Optional[str], optional): comment on the procedure. Defaults to None.
            schedule (Optional[SchedJob], optional): schedule for the procedure. Defaults to None.
        """
        self.name = name
        self.statement = statement
        self.comment = comment
        if schedule:
            schedule.name = name
        self.schedule = schedule

    @staticmethod
    def list_all(
        engine: Engine, schema: Optional[str] = None, name_pattern: Optional[str] = None
    ) -> List[str]:
        """Return a list of existing procedures.

        Args:
            schema (Optional[str], optional): Only select procedures in this schema. Defaults to None.
            name_pattern (Optional[str], optional): Only select procedures named LIKE `name_pattern`. Defaults to None.
        """
        query = sa.select(
            fn.concat(
                routines_table.c.routine_schema, ".", routines_table.c.routine_name
            )
        ).where(routines_table.c.routine_type == "PROCEDURE")
        if schema:
            query = query.where(routines_table.c.routine_schema == schema)
        if name_pattern:
            query = query.where(routines_table.c.routine_name.like(name_pattern))
        with engine.begin() as conn:
            procs = list(execute_sql(query, conn).scalars())
        return procs

    def create(self, engine: Engine):
        """Create a procedure in the database."""
        if not self.statement:
            raise ValueError(
                "Can not create procedure. Statement attribute is not set."
            )
        if not isinstance(self.statement, (list, tuple)):
            self.statement = [self.statement]
        with engine.begin() as conn:
            bodies = [
                statement.compile(conn, compile_kwargs={"literal_binds": True})
                if not isinstance(statement, (str, Compiled))
                else statement
                for statement in self.statement
            ]

        statement = " ".join(
            [
                f"CREATE OR REPLACE PROCEDURE {self.name}() LANGUAGE SQL",
                # "PLPGSQL" if len(bodies) > 1 else "SQL",
                "AS $$\n",
                "\n".join([f"{body};" for body in bodies]),
                "\n$$;",
            ]
        )
        execute_sql(statement)
        if self.comment:
            execute_sql(f"COMMENT ON PROCEDURE {self.name} () IS '{self.comment}';")
        if self.schedule:
            self.schedule.create()

    @property
    def details(self) -> Dict[str, Any]:
        schema, name = split_schema_table(self.name)
        details = {
            "name": self.name,
            "schedule": SchedJob.list_all(schema, name),
        }
        if self.comment:
            details["comment"] = self.comment
        return details

    def run(self):
        """Run the procedure."""
        logger.info("Running procedure: %s", self.name)
        execute_sql(f"CALL {self.name}();")

    def drop(self, cascade: bool = True):
        """Drop the procedure from the database.

        Args:
            cascade (bool, optional): Use DROP CASCADE. Defaults to True.
        """

        self.drop(self.name, cascade=cascade)

    @staticmethod
    def drop_procedure(name: str, cascade: bool = True):
        """Drop the procedure from the database.

        Args:
            cascade (bool, optional): Use DROP CASCADE. Defaults to True.
        """
        logger.info("Dropping procedure %s", name)
        statement = [f"DROP PROCEDURE IF EXISTS {name}"]
        if cascade:
            statement.append("CASCADE")
        execute_sql(" ".join(statement))
