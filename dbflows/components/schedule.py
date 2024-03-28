from datetime import datetime
from functools import cached_property
from pprint import pformat
from typing import Any, Dict, List, Optional

import sqlalchemy as sa
from sqlalchemy import func as fn
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.sql.expression import cast

from dbflows.utils import logger, schema_table

from .base import DbObj, query_kwargs

meta = sa.MetaData(schema="timescaledb_information")

jobs_table = sa.Table(
    "jobs",
    meta,
    sa.Column("job_id", sa.Integer),
    sa.Column("application_name", sa.Text),
    sa.Column("schedule_interval", sa.Text),
    sa.Column("max_runtime", sa.Text),
    sa.Column("max_retries", sa.Integer),
    sa.Column("retry_period", sa.Text),
    sa.Column("proc_schema", sa.Text),
    sa.Column("proc_name", sa.Text),
    sa.Column("owner", sa.Text),
    sa.Column("scheduled", sa.Boolean),
    sa.Column("fixed_schedule", sa.Boolean),
    sa.Column("config", JSONB),
    sa.Column("next_start", sa.DateTime(timezone=True)),
    sa.Column("initial_start", sa.DateTime(timezone=True)),
    # sa.Column("hypertable_schema", sa.Text),
    # sa.Column("hypertable_name", sa.Text),
    # sa.Column("check_schema", sa.Text),
    # sa.Column("check_name", sa.Text),
)

job_stats_table = sa.Table(
    "job_stats",
    meta,
    sa.Column("hypertable_schema", sa.Text),
    sa.Column("hypertable_name", sa.Text),
    sa.Column("job_id", sa.Integer),
    sa.Column("last_run_started_at", sa.DateTime(timezone=True)),
    sa.Column("last_successful_finish", sa.DateTime(timezone=True)),
    sa.Column("last_run_status", sa.Text),
    sa.Column("job_status", sa.Text),
    sa.Column("last_run_duration", sa.Interval),
    sa.Column("next_start", sa.DateTime(timezone=True)),
    sa.Column("total_runs", sa.BigInteger),
    sa.Column("total_successes", sa.BigInteger),
    sa.Column("total_failures", sa.BigInteger),
)

job_errors_table = sa.Table(
    "job_errors",
    meta,
    sa.Column("job_id", sa.Integer),
    sa.Column("proc_schema", sa.Text),
    sa.Column("proc_name", sa.Text),
    sa.Column("pid", sa.Integer),
    sa.Column("start_time", sa.DateTime(timezone=True)),
    sa.Column("finish_time", sa.DateTime(timezone=True)),
    sa.Column("sqlerrcode", sa.Text),
    sa.Column("err_message", sa.Text),
)


class SchedJob(DbObj):
    """A process to be ran by Timescale job scheduler."""

    def __init__(
        self,
        name: str = None,
        schedule_interval: str = "1 day",
        initial_start: Optional[datetime] = None,
        config: Optional[Dict[str, Any]] = None,
        fixed_schedule: Optional[bool] = None,
        max_runtime: Optional[str] = None,
        max_retries: Optional[int] = 1,
        retry_period: Optional[str] = None,
        create_replace: Optional[bool] = None,
    ) -> None:
        """
        Args:
            name (str, optional): Name of the function or procedure to register as job. Defaults to None.
            schedule_interval (Optional[str], optional): Interval between executions of this job. Defaults to 24 hours.. Defaults to None.
            initial_start (Optional[datetime], optional): Time the job is first run. In the case of fixed schedules, this also serves as the origin on which job executions are aligned. If omitted, the current time is used as origin in the case of fixed schedules. Defaults to None. Defaults to None.
            config (Optional[Dict[str, Any]], optional): Job-specific configuration, passed to the function when it runs. Defaults to None.
            fixed_schedule (Optional[bool], optional): Set to FALSE if you want the next start of a job to be determined as its last finish time plus the schedule interval. Set to TRUE if you want the next start of a job to begin schedule_interval after the last start. Defaults to TRUE.
            max_runtime (Optional[str], optional): (INTERVAL) The maximum amount of time the job is allowed to run by the background worker scheduler before it is stopped. Defaults to None.
            max_retries (Optional[int], optional): The number of times the job is retried if it fails. Defaults to 1.
            retry_period (Optional[str], optional): (INTERVAL) The amount of time the scheduler waits between retries of the job on failure. Defaults to None.
        """
        self.name = name
        self.schedule_interval = schedule_interval
        self.initial_start = initial_start
        self.config = config
        self.fixed_schedule = fixed_schedule
        self.max_runtime = max_runtime
        self.max_retries = max_retries
        self.retry_period = retry_period
        self.create_replace = create_replace
        self._job_id: int = None

    @cached_property
    def name(self) -> str:
        details = self.details()
        return f'{details["proc_schema"]}.{details["proc_name"]}'

    @property
    def job_id(self, engine: Engine) -> int:
        """Get the job id for this scheduled process/function."""
        if self._job_id is None:
            query = sa.select(jobs_table.c.job_id).where(
                jobs_table.c.proc_name == self.name
            )
            with engine.begin() as conn:
                job_id = execute_sql(query, conn).scalar()
            self._job_id = job_id
        return self._job_id

    def create(self, engine: Engine, timezone: str):
        """Create the scheduled job."""
        # TODO default to db timezone?

        if self.job_id is None:
            # job with this name does not exist, so create it.
            if self.name is None:
                raise RuntimeError(
                    "Job name attribute must be set before creating job."
                )
            kwargs = {
                k: f"'{v}'"
                for k, v in (
                    ("proc", self.name),
                    ("timezone", timezone),
                    ("schedule_interval", self.schedule_interval),
                )
            }
            if self.initial_start:
                inital_start = self.initial_start.strftime("%Y-%m-%d %H:%M:%S")
                kwargs["initial_start"] = f"'{inital_start}'::sa.DateTimetz"
            if self.fixed_schedule:
                kwargs["fixed_schedule"] = str(self.fixed_schedule).lower()
            logger.info("Creating job:\n%s.", pformat(kwargs))
            with engine.begin() as conn:
                # returns the job id.
                self._job_id = execute_sql(
                    f"SELECT add_job({query_kwargs(kwargs)});", conn
                ).scalar()
        elif self.create_replace is None:
            logger.info("Job '%s' already exists, skipping creation.", self.name)
        elif self.create_replace:
            self.drop()
        else:
            raise RuntimeError(f"Job '{self.name}' already exists.")
        # check if parameters need to be altered.
        kwargs = {}
        if self.max_runtime:
            kwargs["max_runtime"] = f"'{self.max_runtime}'"
        if self.max_retries:
            kwargs["max_retries"] = self.max_retries
        if self.retry_period:
            kwargs["retry_period"] = f"'{self.retry_period}'"
        if kwargs:
            logger.info("Updating job '%s':\n%s.", self.name, pformat(kwargs))
            execute_sql(
                f"SELECT alter_job({self.job_id},{query_kwargs(kwargs)});", engine
            )

    def drop(self, engine: Engine):
        """Delete previously created scheduled job."""
        self._drop(engine, self.name, self.job_id)

    @staticmethod
    def drop_matching(schema: Optional[str] = None, name_pattern: Optional[str] = None):
        for job in SchedJob.details(schema, name_pattern):
            SchedJob._drop(f'{job["proc_schema"]}.{job["proc_name"]}', job["job_id"])

    def set_enabled(self, engine: Engine, enabled: bool):
        """Enable/disable the scheduled job."""
        execute_sql(
            f"SELECT alter_job({self.job_id}, scheduled => {str(enabled).lower()});",
            engine,
        )

    def run(self, engine: Engine):
        """Run the job immediately."""
        execute_sql(f"CALL run_job({self.job_id});", engine)

    def details(self, engine: Engine) -> Dict[str, Any]:
        schema, table = schema_table(self.name).split(".")
        if proc_details := SchedJob.list_all(engine, schema, table):
            assert len(proc_details) == 1
            return proc_details[0]

    @staticmethod
    def list_all(
        engine: Engine, schema: Optional[str] = None, name_pattern: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        # shows information about jobs registered with the automation framework.
        query = sa.select(
            fn.concat(jobs_table.c.proc_schema, ".", jobs_table.c.proc_name).label(
                "name"
            ),
            jobs_table.c.job_id,
            *SchedJob._select_columns(jobs_table),
            *SchedJob._select_columns(job_stats_table),
        ).select_from(
            jobs_table.join(
                job_stats_table, jobs_table.c.job_id == job_stats_table.c.job_id
            )
        )
        if schema:
            query = query.where(jobs_table.c.proc_schema == schema)
        if name_pattern:
            query = query.where(jobs_table.c.proc_name.like(name_pattern))
        with engine.begin() as conn:
            procs_details = [dict(r._mapping()) for r in conn.execute(query).fetchall()]

        for pd in procs_details:
            query = (
                sa.select(*SchedJob._select_columns(job_errors_table))
                .where(job_errors_table.c.job_id == pd["job_id"])
                .order_by(job_errors_table.c.finish_time.desc())
                .limit(15)
            )
            with engine.begin() as conn:
                pd["errors"] = [
                    dict(r._mapping()) for r in conn.execute(query).fetchall()
                ]

        if len(procs_details) == 1:
            return procs_details[0]
        return procs_details

    @staticmethod
    def _drop(engine: Engine, name: str, job_id: int):
        """Delete previously created scheduled job."""
        logger.info("Deleting job %i: '%s'", job_id, name)
        execute_sql(f"SELECT delete_job({job_id});", engine)

    @staticmethod
    def _select_columns(table: sa.Table) -> List[sa.Column]:
        ignore_columns = (
            "hypertable_schema",
            "hypertable_name",
            "check_schema",
            "check_name",
            "job_id",
            "proc_schema",
            "proc_name",
            "next_start",
        )
        return [
            (
                c
                if type(c.type) != sa.DateTime
                else fn.nullif(c, cast("'-infinity'", sa.DateTime)).label(c.name)
            )
            for c in table.columns
            if c.name not in ignore_columns
        ]
