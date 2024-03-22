from collections import defaultdict

import pandas as pd
import pytest
import sqlalchemy as sa

from dbflows.export import export_table
from dbflows.meta import ExportMeta


@pytest.mark.parametrize("primary_save_loc", ["s3", "disk"])
@pytest.mark.parametrize("n_workers", [1, 4])
@pytest.mark.parametrize("sa_objs", [False, True])
def test_export_partition_slices(
    add_table_rows,
    save_locations,
    s3_bucket,
    temp_dir,
    engine,
    faker,
    partition_slice_table,
    primary_save_loc,
    n_workers,
    sa_objs,
    check_export_files,
    file_ops,
):
    """Test that entire database table gets exported correctly, and that new data is appended to existing file."""
    table = partition_slice_table
    save_locs = save_locations(primary_save_loc)
    slice_column = table.c.datetime
    file_max_size = "100 kB"
    partitions = ["_".join(faker.words(2, unique=True)) for _ in range(3)]
    for _ in range(3):
        add_table_rows(table, 10_000, partitions)
        export_table(
            table=table if sa_objs else table.name,
            engine=engine,
            save_locs=save_locs,
            slice_column=slice_column if sa_objs else slice_column.name,
            partition_column=table.c.partition if sa_objs else table.c.partition.name,
            file_max_size=file_max_size,
            file_stem_prefix="test",
            n_workers=n_workers,
            fo=file_ops,
        )
        # read partition data from database.
        db_partition_dfs = {}
        for partition in partitions:
            with engine.begin() as conn:
                db_partition_dfs[partition] = pd.read_sql_query(
                    sa.select(table)
                    .where(table.c.partition == partition)
                    .order_by(table.c.datetime.asc()),
                    conn,
                )

        # read exported data from files and check that it is same as database.
        loc_partition_dfs = defaultdict(list)
        for loc in (s3_bucket, temp_dir):
            loc_files = [str(f) for f in file_ops.list_files(loc)]
            for partition in partitions:
                partition_files = [f for f in loc_files if f"_P({partition})" in f]
                assert len(partition_files) > 1
                for file in partition_files:
                    # check file name is as expected.
                    export_file = ExportMeta.from_file(file)
                    check_export_files(
                        export_file=export_file,
                        table=table,
                        slice_column=slice_column,
                        partition=partition,
                        has_max_size=True,
                    )
                    # save dfs for later comparison.
                    loc_partition_dfs[(loc, partition)].append(
                        (
                            export_file,
                            pd.read_csv(
                                file,
                                names=[c.name for c in table.columns],
                                parse_dates=["datetime"],
                                date_format="ISO8601",
                                storage_options=(
                                    file_ops.s3.storage_options
                                    if file_ops.s3.is_s3_path(file)
                                    else None
                                ),
                            ).astype(db_partition_dfs[partition].dtypes),
                        ),
                    )

        for (loc, partition), csv_dfs in loc_partition_dfs.items():
            csv_dfs.sort(key=lambda x: x[0].slice_start)
            # CSV datetime might be object, so cast it.
            csv_df = pd.concat([df for _, df in csv_dfs], ignore_index=True)
            pd.testing.assert_frame_equal(csv_df, db_partition_dfs[partition])
