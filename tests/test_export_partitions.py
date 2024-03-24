import pandas as pd
import pytest
import sqlalchemy as sa

from dbflows.export import export_table
from dbflows.meta import ExportMeta
from fileflows.s3 import is_s3_path


@pytest.mark.parametrize("primary_save_loc", ["s3", "disk"])
@pytest.mark.parametrize("n_workers", [1, 4])
def test_export_partitions(
    add_table_rows,
    save_locations,
    table_partitions,
    engine,
    partition_slice_table,
    primary_save_loc,
    n_workers,
    check_export_files,
    file_ops,
):
    """Test that partitions get exported correctly, and that new data is appended to existing file."""
    save_locs = save_locations(primary_save_loc)
    table = partition_slice_table
    for _ in range(3):
        add_table_rows(table)
        export_table(
            table=table,
            engine=engine,
            save_locs=save_locs,
            # test append if slice column is used, or overwrite if not used.
            slice_column=None,
            partition_column=table.c.partition,
            file_max_size=None,
            file_stem_prefix="test",
            n_workers=n_workers,
            fo=file_ops,
        )
        # there should be one file for each partition.
        # read exported data from files and check that it is same as database.
        for loc in save_locs:
            loc_files = [str(f) for f in file_ops.list_files(loc)]
            for partition in table_partitions:
                partition_file = [f for f in loc_files if f"_P({partition})" in f]
                assert len(partition_file) == 1
                partition_file = partition_file[0]
                check_export_files(
                    export_file=ExportMeta.from_file(partition_file),
                    table=table,
                    slice_column=None,
                    partition=partition,
                )
                # read partition data from database.
                with engine.begin() as conn:
                    db_df = pd.read_sql_query(
                        sa.select(table)
                        .order_by(table.c.datetime.asc())
                        .where(table.c.partition == partition),
                        conn,
                    )
                file_df = pd.read_csv(
                    partition_file,
                    names=[c.name for c in table.columns],
                    parse_dates=["datetime"],
                    date_format="ISO8601",
                    storage_options=(
                        file_ops.s3.storage_options
                        if is_s3_path(partition_file)
                        else None
                    ),
                ).astype(db_df.dtypes)
                # sort for comparisons.
                file_df.sort_values(
                    by="datetime", ascending=True, inplace=True, ignore_index=True
                )
                pd.testing.assert_frame_equal(db_df, file_df)
