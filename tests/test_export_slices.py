import pandas as pd
import pytest
import sqlalchemy as sa
from fileflows.s3 import is_s3_path

from dbflows.export import export
from dbflows.meta import ExportMeta


@pytest.mark.parametrize("primary_save_loc", ["s3", "disk"])
@pytest.mark.parametrize("n_workers", [1, 4])
def test_export_slices(
    add_table_rows,
    save_locations,
    s3_bucket,
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
    slice_column = table.c.datetime
    file_max_size = "100 kB"

    for _ in range(3):
        add_table_rows(table, 10_000)
        export(
            table=table,
            engine=engine,
            save_locs=save_locs,
            # test append if slice column is used, or overwrite if not used.
            slice_column=slice_column,
            partition_column=None,
            file_max_size=file_max_size,
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
        file_dfs = []
        for loc in save_locs:
            loc_files = file_ops.list_files(loc)
            assert len(loc_files) > 1
            for file in loc_files:
                # check file name is as expected.
                export_file = ExportMeta.from_file(file)
                check_export_files(
                    export_file=export_file,
                    table=table,
                    slice_column=slice_column,
                    has_max_size=True,
                )
                # save dfs from one location for later comparison.
                if loc == s3_bucket:
                    file_dfs.append(
                        (
                            export_file,
                            pd.read_csv(
                                file,
                                names=[c.name for c in table.columns],
                                parse_dates=["datetime"],
                                date_format="ISO8601",
                                storage_options=(
                                    file_ops.s3.storage_options
                                    if is_s3_path(file)
                                    else None
                                ),
                            ).astype(db_df.dtypes),
                        )
                    )
        file_dfs.sort(key=lambda x: x[0].slice_start)
        all_files_df = pd.concat([df for _, df in file_dfs], ignore_index=True)
        pd.testing.assert_frame_equal(db_df, all_files_df)
