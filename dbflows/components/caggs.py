from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.functions import _FunctionGenerator

from dbflows.utils import logger, next_time_occurrence, schema_table

from .base import query_kwargs
from .views import MaterializedView

cagg_meta_table = sa.Table(
    "continuous_aggregates",
    sa.MetaData(schema="timescaledb_information"),
    sa.Column("view_schema", sa.Text),
    sa.Column("view_name", sa.Text),
    sa.Column("compression_enabled", sa.Boolean),
    sa.Column("materialized_only", sa.Boolean),
    sa.Column("finalized", sa.Boolean),
)

policy_meta_table = sa.Table(
    "policies",
    sa.MetaData(schema="timescaledb_experimental"),
    sa.Column("relation_schema", sa.Text),
    sa.Column("relation_name", sa.Text),
    sa.Column("schedule_interval", sa.Interval),
    sa.Column("proc_schema", sa.Text),
    sa.Column("proc_name", sa.Text),
    sa.Column("config", JSONB),
)


class time_bucket(TextClause):
    """A TimescaleDB time bucket."""

    def __init__(
        self,
        bucket_width: str,
        time_column: sa.Column,
        timezone: Optional[str] = None,
        label: Optional[str] = None,
    ):
        label = label or "bucket_" + bucket_width.replace(" ", "")
        tb_args = [f"'{bucket_width}'::interval", time_column.name]
        if timezone is not None:
            tb_args.append(f"timezone => '{timezone}'")
        tb_args = ",".join(tb_args)
        self._column = sa.Column(label, sa.DateTime(timezone=True))
        super().__init__(f"""time_bucket({tb_args}) AS "{label}\"""")
        self._label = sa.text(label)

    @property
    def label(self):
        return self._label

    @property
    def c(self):
        return self._column

    @property
    def column(self):
        return self._column


class CAgg(MaterializedView):
    """A TimescaleDB continuous aggregate."""

    def __init__(
        self,
        name: str,
        # TODO text type?
        aggs: List[Union[sa.Column, sa.Text]],
        time_col: sa.Column,
        bucket_width: str,
        additional_group_by: Optional[List[sa.Column]] = None,
        timezone: Optional[str] = None,
        bucket_label: Optional[str] = None,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        create_with_no_data: bool = True,
        replace_existing: Optional[bool] = None,
    ) -> None:
        self.timezone = timezone
        self.schema = schema
        self.time_bucket = time_bucket(bucket_width, time_col, timezone, bucket_label)
        additional_group_by = additional_group_by or []
        if not isinstance(additional_group_by, (list, tuple)):
            additional_group_by = [additional_group_by]
        query = sa.select(
            *additional_group_by,
            self.time_bucket,
            *aggs,
        ).group_by(self.time_bucket.label, *additional_group_by)
        table = sa.Table(
            name,
            sa.MetaData(schema=self.schema),
            self.time_bucket.column,
            *[sa.Column(col.name, col.type) for col in additional_group_by],
            *[sa.Column(col.name, col.type) for col in aggs],
        )
        super().__init__(
            table=table,
            query=query,
            create_with_no_data=create_with_no_data,
            replace_existing=replace_existing,
            storage_params=[
                "timescaledb.continuous",
                "timescaledb.materialized_only = false",
            ],
            comment=comment,
        )

    def compress(self, engine: Engine, order_by: Optional[str] = None):
        """Compress the CAgg. Optionally order_by column name and direction (ASC or DESC). timescaledb.compress_orderby = 'updated DESC'"""
        statement = f"ALTER MATERIALIZED VIEW {schema_table(self.table)} SET (timescaledb.compress=true"
        if order_by:
            statement += f",timescaledb.compress_orderby = '{order_by}'"
        statement += ")"
        execute_sql(statement, engine)

    @staticmethod
    def compression_policies(engine: Engine) -> List[Dict[str, Any]]:
        with engine.begin() as conn:
            policies = conn.execute(
                sa.text(
                    "SELECT * FROM timescaledb_information.jobs WHERE proc_name='policy_compression';"
                )
            ).fetchall()
            policies = [dict(r._mapping) for r in policies]
        return policies

    # TODO decompress? SELECT remove_compression_policy('table name');

    def add_refresh_policy(
        self,
        engine: Engine,
        refresh_start_offset: Optional[str] = None,
        refresh_end_offset: Optional[str] = None,
        refresh_schedule_interval: str = "1 day",
        refresh_initial_start: Optional[datetime] = None,
        replace_existing: bool = True,
    ):
        """Add a refresh policy to the Continuous Aggregate.

        Args:
            engine (Engine): _description_
            refresh_start_offset (Optional[str], optional): INTERVAL	Start of the refresh window as an interval relative to the time when the policy is executed. NULL is equivalent to MIN(timestamp) of the hypertable. Defaults to None.
            refresh_end_offset (Optional[str], optional): End of the refresh window as an interval relative to the time when the policy is executed. NULL is equivalent to MAX(timestamp) of the hypertable. Defaults to None.
            refresh_schedule_interval (str, optional): Interval between refresh executions in wall-clock time. Defaults to 24 hours. Defaults to "1 day".
            refresh_initial_start (Optional[datetime], optional): TIMESTAMPTZ Time the policy is first run. Defaults to NULL. If omitted, then the schedule interval is the interval between the finish time of the last execution and the next start. If provided, it serves as the origin with respect to which the next_start is calculated. Defaults to None.
            replace_existing (bool, optional): _description_. Defaults to True.
        """
        # TODO select add_policy stuff
        if self.refresh_policy(engine):
            if not replace_existing:
                logger.info(
                    "Policy already exists for %s. Will not recreate.", self.name
                )
                return
            self.remove_refresh_policy(engine)
        kwargs = {
            "start_offset": refresh_start_offset,
            "end_offset": refresh_end_offset,
            "schedule_interval": refresh_schedule_interval,
            "initial_start": (
                refresh_initial_start or next_time_occurrence(hour=3, minute=30)
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": self.timezone,
        }
        kwargs = {k: f"'{v}'" for k, v in kwargs.items() if v is not None}
        for k in ("start_offset", "end_offset"):
            if k not in kwargs:
                kwargs[k] = "NULL"
        logger.info("Adding refresh policy to %s: %s", self.name, kwargs)
        execute_sql(
            f"SELECT add_continuous_aggregate_policy('{schema_table(self.table)}', {query_kwargs(kwargs)})",
            engine,
        )

    def remove_refresh_policy(self, engine: Engine, if_exists: bool = True):
        execute_sql(
            f"SELECT remove_continuous_aggregate_policy('{schema_table(self.table)}', if_exists => {str(if_exists).lower()})",
            engine,
        )

    def refresh_policy(self, engine: Engine) -> Dict[str, Any]:
        return self.cagg_refresh_policy(schema_table(self.table), engine)

    @staticmethod
    def cagg_refresh_policy(engine: Engine, name: str) -> Dict[str, Any]:
        with engine.begin() as conn:
            return conn.execute(
                sa.text(f"SELECT timescaledb_experimental.show_policies('{name}')")
            ).scalar()

    @staticmethod
    def agg_col(
        agg_col_prefix: str,
        formula: Any,
        accessor: _FunctionGenerator,
        aggregate: Optional[_FunctionGenerator] = None,
        label: Optional[str] = None,
    ) -> sa.Column:
        label = CAgg._filter_name(
            label or f"{agg_col_prefix}_{accessor._FunctionGenerator__names[0]}"
        )
        return accessor(aggregate(formula) if aggregate else formula).label(label)

    def details(self, engine: Engine) -> Dict[str, Any]:
        if details := self.list_all(engine, self.schema, self.name):
            assert len(details) == 1
            return details[0]

    @staticmethod
    def list_all(
        engine: Engine,
        schema: Optional[str] = None,
        like_pattern: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query = sa.select(cagg_meta_table)
        if schema:
            query = query.where(cagg_meta_table.c.view_schema == schema)
        if like_pattern:
            query = query.where(cagg_meta_table.c.view_name.like(like_pattern))
        with engine.begin() as conn:
            details = [dict(r._mapping) for r in conn.execute(query).fetchall()]
        details = {f"{d.pop('view_schema')}.{d.pop('view_name')}": d for d in details}
        for name, info in details.items():
            info["policy"] = CAgg.refresh_policy(name, engine)
        return details
