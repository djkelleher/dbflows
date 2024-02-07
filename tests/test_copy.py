import pandas as pd
import pytest
import sqlalchemy as sa

from dbgress.utils import copy_to_csv


@pytest.mark.parametrize("suffix", [".csv", ".csv.gz"])
@pytest.mark.parametrize("to_copy", ["table", "select"])
def test_copy_to_csv(
    engine,
    temp_dir,
    add_table_rows,
    partition_slice_table,
    suffix,
    to_copy,
):
    """Test that a table or query gets exported to CSV file."""
    table = partition_slice_table
    if to_copy == "select":
        query = to_copy = sa.select(table).order_by(table.c.datetime).limit(200)
    else:
        query = sa.select(table)
        to_copy = table
    # add rows to table.
    add_table_rows(table)
    # read data from database.
    with engine.begin() as conn:
        db_df = pd.read_sql_query(query, conn)
    save_path = temp_dir / f"{table.name}{suffix}"
    copy_to_csv(
        to_copy=to_copy,
        save_path=save_path,
        engine=engine,
        append=True,
    )
    file_df = pd.read_csv(
        save_path,
        names=[c.name for c in table.columns],
        parse_dates=["datetime"],
    ).astype(db_df.dtypes)
    pd.testing.assert_frame_equal(db_df, file_df)


@pytest.mark.parametrize("suffix", [".csv", ".csv.gz"])
def test_append_csv(engine, temp_dir, add_table_rows, partition_slice_table, suffix):
    """Test that new data is appended to existing file."""
    table = partition_slice_table
    add_table_rows(table)
    copy_200 = sa.select(table).order_by(table.c.datetime.asc()).limit(200)
    with engine.begin() as conn:
        greatest_time = conn.execute(
            sa.select(sa.func.max(sa.column("datetime"))).select_from(
                copy_200.subquery()
            )
        ).scalar()

    save_path = temp_dir / f"{table.name}{suffix}"
    # copy first 200 rows.
    copy_to_csv(
        to_copy=copy_200,
        save_path=save_path,
        engine=engine,
        append=True,
    )
    # copy all remaining rows.
    copy_to_csv(
        to_copy=sa.select(table)
        .where(table.c.datetime > greatest_time)
        .order_by(table.c.datetime.asc()),
        save_path=save_path,
        engine=engine,
        append=True,
    )
    # read data from database.
    with engine.begin() as conn:
        db_df = pd.read_sql_query(
            sa.select(table).order_by(table.c.datetime.asc()),
            conn,
        )
    # read data from file.
    file_df = pd.read_csv(
        save_path,
        names=[c.name for c in table.columns],
        parse_dates=["datetime"],
    ).astype(db_df.dtypes)
    pd.testing.assert_frame_equal(db_df, file_df)
