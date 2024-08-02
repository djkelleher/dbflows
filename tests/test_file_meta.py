from pathlib import Path
from random import randint
from zoneinfo import ZoneInfo

import pytest

from dbflows.export.hive import ExportMeta, _create_file_name_re


@pytest.fixture
def random_file_path(faker):
    # create a random file name.
    prefix = "_".join(faker.words(randint(2, 4)))
    table = "_".join(faker.words(randint(2, 4)))
    partition = "_".join(faker.words(randint(2, 4)))
    slice_start = faker.date_time_between(start_date="-1y", end_date="now").replace(
        tzinfo=ZoneInfo("UTC")
    )
    slice_end = faker.date_time_between(start_date="-3y", end_date="-2y").replace(
        tzinfo=ZoneInfo("UTC")
    )
    file = Path(
        f"/some/dir/{prefix}_T({table})_P({partition})_{slice_start.isoformat()}_{slice_end.isoformat()}.csv.gz"
    )
    return table, prefix, partition, slice_start, slice_end, file


def test_file_stem_re(random_file_path):
    reg = _create_file_name_re()
    # test allowed file name formats.
    table, prefix, partition, slice_start, slice_end, file = random_file_path
    match = reg.search(file.name)
    assert match is not None
    assert match.groupdict() == {
        "prefix": prefix,
        "schema_table": table,
        "partition": partition,
        "slice_range": f"{slice_start.isoformat()}_{slice_end.isoformat()}",
        "suffix": ".csv.gz",
    }
    name = f"{prefix}_T({table})_P({partition}).csv.gz"
    match = reg.search(name)
    assert match is not None
    assert match.groupdict() == {
        "prefix": prefix,
        "schema_table": table,
        "partition": partition,
        "slice_range": None,
        "suffix": ".csv.gz",
    }
    name = f"{prefix}_T({table}).csv.gz"
    match = reg.search(name)
    assert match is not None
    assert match.groupdict() == {
        "prefix": prefix,
        "schema_table": table,
        "partition": None,
        "slice_range": None,
        "suffix": ".csv.gz",
    }
    name = f"T({table}).csv.gz"
    match = reg.search(name)
    assert match is not None
    assert match.groupdict() == {
        "prefix": None,
        "schema_table": table,
        "partition": None,
        "slice_range": None,
        "suffix": ".csv.gz",
    }


def test_file_name_parse(random_file_path):
    # check that attributes are correctly parsed from file name.
    table, prefix, partition, slice_start, slice_end, file = random_file_path
    meta = ExportMeta.from_file(file)
    assert meta.schema_table == f"public.{table}"
    assert meta.slice_start == slice_start
    assert meta.slice_end == slice_end
    assert meta.partition == partition
    assert meta.prefix == prefix
    assert meta.statement is None
    assert meta.path is file
    # check that the file gets constructed from attributes.
    assert meta.create_file_name() == file.name.replace(table, meta.schema_table)
