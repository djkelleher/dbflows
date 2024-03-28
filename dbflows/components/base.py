import re
from functools import cached_property
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from dbflows.utils import driver_pg_url, logger

metric_abbrs = {
    "average": "avg",
    "kurtosis": "kurt",
    "stddev": "std",
}


def query_kwargs(kwargs) -> str:
    return ",".join([f"{k} => {v}" for k, v in kwargs.items()])


class DbObj:
    def __init__(self, pg_url: str) -> None:
        self.pg_url = driver_pg_url(driver="psycopg", url=pg_url)

    @cached_property
    def engine(self) -> Engine:
        return sa.create_engine(self.pg_url)

    @property
    def name(self) -> str:
        raise NotImplementedError(
            f"`name` property not implemented for {self.__class__.__name__}"
        )

    def execute_sql(self, sql: Any):
        """Execute a SQL statement."""
        logger.info(sql)
        if isinstance(sql, str):
            sql = sa.text(sql)
        with self.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            return conn.execute(sql)

    def create(self):
        raise NotImplementedError(
            f"`create` method not implemented for {self.__class__.__name__}"
        )

    def drop(self):
        raise NotImplementedError(
            f"`drop` method not implemented for {self.__class__.__name__}"
        )

    @staticmethod
    def list_all(
        schema: Optional[str] = None, like_pattern: Optional[str] = None
    ) -> List[str]:
        raise NotImplementedError("`list_all` method not implemented.")

    @staticmethod
    def _filter_name(name: str) -> str:
        name = re.sub(r"\s+", "_", name).lower()
        for metric, abbr in metric_abbrs.items():
            name = name.replace(metric, abbr)
        return name

    def __repr__(self) -> str:
        kwargs = {"name": self.name}
        if comment := getattr(self, "comment", None):
            kwargs["comment"] = comment
        return f"{self.__class__.__name__}({query_kwargs(kwargs)})"
