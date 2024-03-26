import re
from datetime import date, datetime, timedelta
from pathlib import Path
from subprocess import run
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from pydantic import PostgresDsn, validate_call
from quicklogs import get_logger
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Compiled, Engine
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm.decl_api import DeclarativeMeta

logger = get_logger("dbflows", stdout=True)

TimeT = Union[int, float, datetime, date]


_ws_re = re.compile(r"\s+")
_camel_re = re.compile(r"([a-z0-9])([A-Z])")
_digit_start_re = re.compile(r"^\d")


def to_snake_case(name: str) -> str:
    """Convert `name` to snake case and also add a leading underscore to variables starting with a digit.

    Args:
        name (str): The name to convert.

    Returns:
        str: The converted name.
    """
    # remove trailing space.
    name = name.strip()
    # convert space to underscores.
    name = _ws_re.sub("_", name)
    # convert camel case to underscores.
    if not name.isupper():
        name = _camel_re.sub(lambda m: f"{m.group(1)}_{m.group(2)}", name)
    # variable names can't start with number, so add leading underscore.
    if _digit_start_re.match(name):
        name = f"_{name}"
    # make name lowercase.
    return name.lower()


def to_seconds(time: float) -> float:
    # shift decimal if time is in units other than milisecond.
    # epoc in miliseconds is 13 digits long.
    return time * 10 ** (10 - len(str(time).split(".")[0]))


def to_miliseconds(time: float) -> float:
    # shift decimal if time is in units other than milisecond.
    # epoc in miliseconds is 13 digits long.
    return time * 10 ** (13 - len(str(time).split(".")[0]))


def truncate_pg_bigint(v) -> int:
    """Check that value is within the range of a Postgres bigint."""
    return min(int(v), 9223372036854775807)


def truncate_pg_float(v) -> float:
    """Check that value is within the range of a Postgres float."""
    return min(float(v), 1e308)


def next_time_occurrence(
    hour: int,
    minute: int = 0,
    second: int = 0,
    tz: Optional[Union[str, ZoneInfo]] = None,
) -> datetime:
    """Return the next datetime at time with specified hour/minute."""
    now = datetime.now()
    if tz:
        if isinstance(tz, str):
            tz = ZoneInfo(tz)
        now = now.astimezone(tz)
    if now.hour >= hour:
        now += timedelta(days=1)
    return now.replace(hour=hour, minute=minute, second=second, microsecond=0)


def to_table(table: Union[sa.Table, DeclarativeMeta]) -> sa.Table:
    """Extract the SQLAlchemy table from an entity, or return the passed argument if argument is already a table.

    Args:
        table (Union[sa.Table, DeclarativeMeta]): An entity or table object.

    Raises:
        ValueError: If argument is not an entity or table object.

    Returns:
        sa.Table: The table corresponding to the passed argument.
    """
    if isinstance(table, sa.Table):
        return table
    elif hasattr(table, "__table__"):
        return table.__table__
    raise ValueError(
        f"Object {table} is not an entity or table! Can not extract table."
    )


def execute_sql(sql: Any, engine: Engine):
    # TODO move this?
    """Execute a SQL statement."""
    logger.info(sql)
    if isinstance(sql, str):
        sql = sa.text(sql)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        return conn.execute(sql)


def compile_sa_statement(statement: Any) -> str:
    """Compile a SQLAlchemy statement and bind query parameters."""
    return str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )


def column_type_casts(
    table: Union[sa.Table, DeclarativeMeta],
    type_casts: Union[Dict[str, Any], Sequence[Any]] = (int, float, str),
) -> Dict[str, Callable[[Any], Any]]:
    # TODO combine functions.
    """Find functions to cast column values to the appropriate Python type.
    This is needed before inserting data into the database.

    Args:
        table (Union[sa.Table, DeclarativeMeta]): Table or entity that needs type casting.
        type_casts (Sequence[Any]): Types that we are interested in casted.

    Returns:
        Dict[str, Any]: Map column name to cast function.

    """
    table = to_table(table)
    type_casts_id_dict = isinstance(type_casts, dict)
    column_casts = {}
    for column in table.columns:
        if (
            col_t := getattr(column.type, "python_type", type(column.type))
        ) in type_casts:
            column_casts[column.name] = (
                type_casts[col_t] if type_casts_id_dict else col_t
            )
    return column_casts


def split_schema_table(table: str) -> Tuple[str, str]:
    """Split a schema.table string into schema and table name."""
    parts = table.split(".")
    if (n_parts := len(parts)) == 2:
        return parts
    if n_parts == 1:
        return "public", table
    raise ValueError(
        f"Invalid table name: {table}. Expected 'schema.table' or 'table'."
    )


def schema_table(table: sa.Table) -> str:
    """Get the schema qualified table name (format: schema.table)"""
    if not isinstance(table, sa.Table):
        if not hasattr(table, "__table__"):
            raise ValueError(f"Invalid table type ({type(table)}): {table}")
        table = table.__table__
    return f"{table.schema or 'public'}.{table.name}"


def schema_tables(schema: str, engine: Union[str, Engine]) -> List[sa.Table]:
    """Get all tables in a schema."""
    if isinstance(engine, str):
        engine = sa.create_engine(engine)
    meta = sa.MetaData(schema=schema)
    with engine.begin() as conn:
        meta.reflect(bind=conn)
    return list(meta.tables.values())


def remove_engine_driver(url: str) -> str:
    """Remove the 'postgresql+...' driver from a SQLAlchemy URL."""
    return re.sub(r"^postgresql\+\w+:", "postgresql:", url)


def engine_url(engine: Engine) -> str:
    """Get the SQLAlchemy engine URL."""
    if isinstance(engine, Engine):
        return str(engine.url).replace("***", engine.url.password)
    return engine


@validate_call
def parse_pg_url(url: PostgresDsn) -> PostgresDsn:
    """Validate and parse a Postgresql URL."""
    return url


def driver_pg_url(driver: str, url: str) -> str:
    return url.replace("postgresql://", f"postgresql+{driver}://")


def range_slices(
    start: TimeT, end: TimeT, periods: int
) -> List[Tuple[datetime, datetime]]:
    """Split range (end - start) into `periods`."""
    step = (end - start) / periods
    ranges = []
    while start < end:
        ranges.append((start, min(start + step, end)))
        start += step
    if (n_rng := len(ranges)) > periods:
        assert n_rng == periods + 1
        del ranges[-1]
        ranges[-1] = (ranges[-1][0], end)
    return ranges


def query_str(query: Any, engine: Engine) -> str:
    """Bind query parameter and compile SQLAlchemy query to string."""
    if not isinstance(query, (str, Compiled)):
        with engine.begin() as conn:
            query = query.compile(conn, compile_kwargs={"literal_binds": True})
    return re.sub(r"\s+", " ", str(query)).strip()


def pg_tz(engine) -> str:
    with engine.begin() as conn:
        return conn.execute(sa.text("SELECT current_setting('TIMEZONE');")).scalar()


async def set_pg_timezone(
    pg_engine: AsyncEngine, database: str, timezone: str
) -> ZoneInfo:
    """Set the database timezone and return zoneinfo instance with that timezone."""
    async with pg_engine.begin() as conn:
        conn.execute(
            sa.text(f"ALTER DATABASE {database} SET timezone TO '{timezone}';")
        )
    async with pg_engine.begin() as conn:
        conn.execute(sa.text("SELECT pg_reload_conf();"))
    async with pg_engine.begin() as conn:
        db_timezone = conn.execute(
            sa.text("SELECT current_setting('TIMEZONE');")
        ).scalar()
    if db_timezone != timezone:
        raise RuntimeError(
            f"Failed to set database timezone to {timezone}. (currently set to:{db_timezone})"
        )
    return ZoneInfo(db_timezone)


def table_updatable_columns(table: sa.Table) -> Set[str]:
    key_cols = list(table.primary_key.columns)
    return {c.name for c in table.columns if c not in key_cols}


def copy_to_csv(
    to_copy: Union[str, sa.Table, sa.select],
    save_path: Union[Path, str],
    engine: Union[str, Engine],
    append: bool,
):
    """Copy a table or query result to a csv file."""
    if isinstance(engine, str):
        engine = sa.create_engine(engine)
    to_copy = (
        schema_table(to_copy)
        if isinstance(to_copy, sa.Table)
        else f"({query_str(to_copy, engine)})"
    )
    save_path = Path(save_path)
    if not save_path.exists() and save_path.suffix != ".gz":
        copy_to = f"'{save_path}'"
    else:
        program = "gzip" if save_path.suffix == ".gz" else "cat"
        operator = ">>" if append else ">"
        copy_to = f"""PROGRAM '{program} {operator} "{save_path}"'"""
    save_path.parent.mkdir(parents=True, exist_ok=True)
    db_url = remove_engine_driver(engine_url(engine))
    psql_code = " ".join(
        [r"\copy", to_copy, "TO", copy_to, "DELIMITER ',' CSV"]
    ).replace('"', '\\"')
    cmd = f"""psql "{db_url}" -c "{psql_code}\""""
    # cmd = f"COPY ({to_copy}) TO {copy_to} DELIMITER ',' CSV HEADER"
    logger.info("Copying to CSV: %s", cmd)
    result = run(cmd, capture_output=True, text=True, shell=True)
    if err := result.stderr:
        logger.error(err)
    if info := result.stdout:
        logger.debug(info)


def duckdb_table_to_csv(conn, export_dir, table_name: str):
    conn.sql(
        f"COPY (SELECT * FROM {table_name} order by time desc) TO '{export_dir}/{table_name}.csv' (HEADER, DELIMITER ',');"
    )
