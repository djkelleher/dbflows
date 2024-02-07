import pandas as pd
import pytest
import sqlalchemy as sa

from dbgress.transfer import DFFilter, copy_table_data


def data_col_cvt(col: pd.Series) -> pd.Series:
    return col * 2


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("upsert", [False, True])
@pytest.mark.parametrize("filter_df", [False, True])
@pytest.mark.parametrize("finished_tasks_file", [False, True])
@pytest.mark.parametrize(
    "slice_col_partition_col", [(False, True), (True, False), (True, True)]
)
def test_copy_table_data(
    stream,
    upsert,
    filter_df,
    finished_tasks_file,
    slice_col_partition_col,
    engine,
    partition_slice_table_creator,
    add_table_rows,
    temp_file,
):
    slice_col, partition_col = slice_col_partition_col
    is_parallel = slice_col or partition_col
    df_filter = (
        DFFilter(column_converters={"data": data_col_cvt}) if filter_df else None
    )
    with partition_slice_table_creator(
        engine
    ) as src_table, partition_slice_table_creator(engine) as dst_table:
        for i in range(2 if upsert else 1):
            add_table_rows(
                table=src_table,
                n_rows=2500 if is_parallel else 100,
                partitions=["p1", "p2", "p3"],
            )
            with engine.begin() as conn:
                src_df = pd.read_sql_query(
                    sa.select(src_table).order_by(src_table.c.datetime.asc()), conn
                )
            copy_table_data(
                src=src_table,
                dst_table=dst_table,
                src_engine=engine,
                dst_engine=engine,
                upsert=upsert,
                df_filter=df_filter,
                stream=stream,
                slice_column=src_table.c.datetime if slice_col else None,
                partition_column=src_table.c.partition if partition_col else None,
                task_max_size="20kb" if is_parallel else None,
                finished_tasks_file=temp_file if finished_tasks_file else None,
            )
            if finished_tasks_file:
                assert temp_file.read_text().splitlines()
            with engine.begin() as conn:
                dst_df = pd.read_sql_query(
                    sa.select(dst_table).order_by(dst_table.c.datetime.asc()), conn
                )
            if not finished_tasks_file or i == 0 or slice_col:
                if filter_df:
                    pd.testing.assert_frame_equal(dst_df, df_filter.filter_df(src_df))
                else:
                    pd.testing.assert_frame_equal(dst_df, src_df)
            elif finished_tasks_file and i == 1:
                assert len(dst_df) == len(src_df) / 2
