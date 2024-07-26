import os
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from random import choice, randint, uniform
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Callable, List, Literal, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

import asyncpg
import pytest
import pytest_asyncio
import sqlalchemy as sa
from faker import Faker
from fileflows import Files, S3Cfg
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.ext.asyncio.engine import AsyncEngine

from dbflows.meta import ExportMeta
from dbflows.utils import driver_pg_url, schema_table


def pytest_addoption(parser):
    parser.addoption(
        "--pg-url",
        action="store",
        help="URL to a PostgreSQL database that should be used for testing. A temporary schema will be created in this database.",
    )
    parser.addoption(
        "--minio-endpoint",
        action="store",
        help="URL to MinIO/S3 server.",
    )
    parser.addoption(
        "--minio-user",
        action="store",
        help="Username for MinIO.",
    )
    parser.addoption(
        "--minio-password",
        action="store",
        help="Password for MinIO.",
    )


@pytest.fixture
def storage_options(request):
    # TODO use this.
    return {
        "key": request.config.getoption("--minio-user"),
        "secret": request.config.getoption("--minio-password"),
        "client_kwargs": {"endpoint_url": request.config.getoption("--minio-endpoint")},
    }


@pytest_asyncio.fixture
async def temp_db(request) -> str:
    """Create a temporary database and return its URL."""
    pg_url = request.config.getoption("--pg-url")
    base_url, db_name = os.path.split(pg_url)
    test_db_name = f"test_{db_name}"
    conn = await asyncpg.connect(dsn=f"{base_url}/postgres")
    await conn.execute(f"DROP DATABASE IF EXISTS {test_db_name} WITH (FORCE)")
    await conn.execute(f"CREATE DATABASE {test_db_name}")
    # provide URL to the created database.
    yield f"{base_url}/{test_db_name}"
    # drop the database.
    await conn.execute(f"DROP DATABASE {test_db_name} WITH (FORCE)")
    await conn.close()


@pytest.fixture
def engine(temp_db) -> AsyncEngine:
    temp_db = driver_pg_url(driver="psycopg", url=temp_db)
    # return create_async_engine(temp_db)
    return sa.create_engine(temp_db)


@pytest.fixture
def faker() -> Faker:
    return Faker()


@pytest.fixture
def random_underscore_name(faker) -> str:
    def _random_underscore_name() -> str:
        return "_".join(
            [*faker.words(2, unique=True), str(uuid4()).replace("-", "_")]
            # faker.words(2, unique=True)
        ).lower()

    return _random_underscore_name


@pytest.fixture
def random_hyphen_name(faker) -> str:
    def _random_hyphen_name() -> str:
        return "-".join([*faker.words(2, unique=True), str(uuid4())]).lower()
        # return "-".join(faker.words(2, unique=True)).lower()

    return _random_hyphen_name


@pytest.fixture
def file_ops(request) -> Files:
    return Files(
        s3_cfg=S3Cfg(
            s3_endpoint_url=request.config.getoption("--minio-endpoint"),
            aws_access_key_id=request.config.getoption("--minio-user"),
            aws_secret_access_key=request.config.getoption("--minio-password"),
        )
    )


@pytest.fixture
def s3_bucket(random_hyphen_name, file_ops) -> str:
    """Create an s3/MinIO bucket."""

    bucket_name = random_hyphen_name()
    bucket = file_ops.s3.resource.Bucket(bucket_name)
    bucket.create()
    yield f"s3://{bucket_name}"
    for obj in bucket.objects.all():
        obj.delete()
    bucket.delete()


@pytest.fixture
def temp_dir() -> Path:
    """Create a temporary directory and return its path."""
    with TemporaryDirectory() as td:
        yield Path(td)


@pytest.fixture
def temp_file() -> Path:
    with NamedTemporaryFile() as tf:
        yield Path(tf.name)


@pytest.fixture
def save_locations(s3_bucket, temp_dir) -> Callable[[str], List[str]]:
    def _save_locations(primary_save_loc: Literal["s3", "disk"]) -> List[str]:
        if primary_save_loc == "s3":
            return [s3_bucket, temp_dir]
        elif primary_save_loc == "disk":
            return [temp_dir, s3_bucket]

    return _save_locations


@pytest.fixture
def partition_slice_table_creator(random_underscore_name):
    @contextmanager
    def _partition_slice_table_creator(engine):
        tbl = sa.Table(
            random_underscore_name(),
            sa.MetaData(),
            sa.Column("partition", sa.Text, primary_key=True),
            sa.Column("datetime", TIMESTAMP(precision=6), primary_key=True),
            sa.Column("data", sa.Float),
        )
        with engine.begin() as conn:
            tbl.create(conn)
        yield tbl
        with engine.begin() as conn:
            tbl.drop(conn)

    return _partition_slice_table_creator


@pytest.fixture
def partition_slice_table(partition_slice_table_creator, engine) -> sa.Table:
    with partition_slice_table_creator(engine) as table:
        yield table


@pytest.fixture
def table_partitions(faker) -> List[str]:
    return faker.words(randint(3, 6), unique=True)


table_rows_start = defaultdict(
    lambda: datetime(
        year=2000,
        month=1,
        day=1,
        minute=randint(0, 59),
        second=randint(0, 59),
        microsecond=randint(0, 1_000_000),
        tzinfo=ZoneInfo("UTC"),
    ),
)


@pytest.fixture
def add_table_rows(engine, table_partitions) -> Callable[[int, sa.Table], None]:
    """Create a function that can be used to add random rows to a table."""

    def _add_table_rows(
        table: sa.Table,
        n_rows: Optional[int] = None,
        partitions: Optional[List[str]] = None,
    ) -> int:
        n_rows = n_rows or randint(300, 500)
        partitions = partitions or table_partitions
        print(f"Adding {n_rows} rows to table.")
        datetimes = [table_rows_start[table.name]]
        for _ in range(n_rows // 3):
            datetimes.append(
                datetimes[-1]
                + timedelta(
                    days=randint(0, 1),
                    hours=randint(0, 23),
                    minutes=randint(0, 59),
                    seconds=randint(0, 59),
                    microseconds=1,
                )
            )
            for _ in range(3):
                # make sure things still work for the smallest resolution.
                datetimes.append(datetimes[-1] + timedelta(microseconds=1))
        rows = [(choice(partitions), time, uniform(1, 1000)) for time in datetimes]
        with engine.begin() as conn:
            conn.execute(sa.insert(table).values(rows))
        table_rows_start[table.name] = rows[-1][1] + timedelta(microseconds=1)
        return n_rows

    return _add_table_rows


@pytest.fixture
def check_export_files(engine):
    """Check that the file metadata has the expected attributes."""

    def _check_export_files(
        export_file: ExportMeta,
        table: sa.Table,
        slice_column: Optional[sa.Column] = None,
        partition: Optional[str] = None,
        has_max_size: bool = False,
    ):
        assert export_file.prefix == "test"
        assert export_file.schema_table == schema_table(table)
        if slice_column is not None:
            if not has_max_size:
                slice_start_query = sa.select(sa.func.min(table.c.datetime))
                slice_end_query = sa.select(sa.func.max(table.c.datetime))
                if partition:
                    slice_start_query = slice_start_query.where(
                        table.c.partition == partition
                    )
                    slice_end_query = slice_end_query.where(
                        table.c.partition == partition
                    )
                with engine.begin() as conn:
                    slice_start = conn.execute(slice_start_query).scalar()
                    slice_end = conn.execute(slice_end_query).scalar()
                assert export_file.slice_start == slice_start
                if export_file.slice_end is not None:
                    assert export_file.slice_end == slice_end
            assert export_file.slice_start < export_file.slice_end
        if partition:
            assert export_file.partition == partition
        else:
            assert export_file.partition is None

    return _check_export_files


@pytest.fixture
def random_str_rows(faker):
    def _random_str_rows(table: sa.Table, n_rows: int):
        column_names = [str(c) for c in table.columns.keys()]
        return [{c: faker.sentence() for c in column_names} for _ in range(n_rows)]

    return _random_str_rows


@pytest.fixture
def single_column_table(faker):
    return sa.Table(
        "_".join(faker.words(randint(1, 2))),
        sa.MetaData(),
        sa.Column("column", sa.Text, primary_key=True),
    )


@pytest.fixture
def key_value_table(faker, engine):
    table = sa.Table(
        "_".join(faker.words(randint(1, 2))),
        sa.MetaData(),
        sa.Column("key", sa.Text, primary_key=True),
        sa.Column("data", sa.Text, nullable=False),
    )
    with engine.begin() as conn:
        table.create(conn)
    yield table
    with engine.begin() as conn:
        table.drop(conn)


@pytest.fixture
def key_multi_value_table(faker, engine):
    table = sa.Table(
        "_".join(faker.words(randint(1, 2))),
        sa.MetaData(),
        sa.Column("key", sa.Text, primary_key=True),
        sa.Column("data1", sa.Text, nullable=False),
        sa.Column("data2", sa.Text, nullable=False),
    )
    with engine.begin() as conn:
        table.create(conn)
    yield table
    with engine.begin() as conn:
        table.drop(conn)
