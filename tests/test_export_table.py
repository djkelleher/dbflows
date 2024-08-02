import pandas as pd
import pytest
import sqlalchemy as sa
from fileflows.s3 import is_s3_path

from dbflows.export.append import export
from dbflows.export.hive import ExportMeta


@pytest.mark.parametrize("primary_save_loc", ["s3", "disk"])
@pytest.mark.parametrize("n_workers", [1, 4])
def test_export(
    add_table_rows,
    save_locations,
    engine,
    partition_slice_table,
    primary_save_loc,
    n_workers,
    check_export_files,
    file_ops,
):
    """Test that entire database table gets exported correctly, and that new data is appended to existing file."""
    table = partition_slice_table
    save_locs = save_locations(primary_save_loc)
    for _ in range(3):
        add_table_rows(table)
        export(
            table=table,
            engine=engine,
            save_locs=save_locs,
            # test append if slice column is used, or overwrite if not used.
            slice_column=None,
            partition_column=None,
            file_max_size=None,
            file_stem_prefix="test",
            n_workers=n_workers,
            fo=file_ops,
        )
        # read exported data from database.
        with engine.begin() as conn:
            db_df = pd.read_sql_query(
                sa.select(table).order_by(table.c.datetime.asc()), conn
            )
        # read exported data from files and check that it is same as database.
        for loc in save_locs:
            loc_files = file_ops.list_files(loc)
            assert len(loc_files) == 1
            file = loc_files[0]
            # check file name is as expected.
            check_export_files(
                export_file=ExportMeta.from_file(file),
                table=table,
            )
            file_df = pd.read_csv(
                file,
                names=[c.name for c in table.columns],
                parse_dates=["datetime"],
                storage_options=(
                    file_ops.s3.storage_options if is_s3_path(file) else None
                ),
            ).astype(db_df.dtypes)
            # sort for comparisons.
            file_df.sort_values(
                by="datetime", ascending=True, inplace=True, ignore_index=True
            )
            pd.testing.assert_frame_equal(db_df, file_df)
