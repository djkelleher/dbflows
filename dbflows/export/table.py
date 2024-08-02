from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal, Optional, Sequence, Union

import sqlalchemy as sa
from fileflows import Files
from fileflows.s3 import S3Cfg, is_s3_path

from dbflows.utils import schema_table, split_schema_table

from .utils import duckdb_copy_to_file


@dataclass
class ExportLocation:
    """Location to save export data file to."""

    save_loc: Union[Path, str]
    # S3 credentials (if save_loc is an S3  path)
    s3_cfg: Optional[S3Cfg] = None

    def __post_init__(self):
        self.save_loc = str(self.save_loc)
        self.files = Files(s3_cfg=self.s3_cfg)

    @property
    def is_s3(self) -> bool:
        return is_s3_path(self.save_loc)

    def create(self):
        """make sure bucket or local directory exists."""
        self.files.create(self.save_loc)


def export_table(
    table: Union[str, sa.Table],
    pg_url: str,
    export_locations: Union[ExportLocation, Sequence[ExportLocation]],
    file_type: Literal["csv", "csv.gz", "parquet"] = "csv",
):
    """Export a table to a file."""
    if not isinstance(export_locations, (list, tuple)):
        export_locations = [export_locations]
    for loc in export_locations:
        loc.create()
    file_name = "/".join(split_schema_table(schema_table(table))) + f".{file_type}"
    # check if any save location is a local directory.
    local_dirs = [l for l in export_locations if not l.is_s3]
    if local_dirs:
        export_loc = local_dirs.pop(0)
        save_path = f"{export_loc.save_loc}/{file_name}"
        duckdb_copy_to_file(
            to_copy=table, save_path=save_path, pg_url=pg_url, s3_cfg=export_loc.s3_cfg
        )
        # copy to other locations.
        s3_dirs = [l for l in export_locations if l.is_s3]
        for loc in local_dirs + s3_dirs:
            loc.files.copy(save_path, f"{loc.save_loc}/{file_name}")
    elif len(export_locations) > 1:
        # save to local directory
        with TemporaryDirectory() as td:
            save_path = f"{td}/{file_name}"
            print(save_path)
            duckdb_copy_to_file(to_copy=table, save_path=save_path, pg_url=pg_url)
            # copy to all s3 destinations
            for loc in export_locations:
                loc.files.copy(save_path, f"{loc.save_loc}/{file_name}")
    else:
        assert len(export_locations) == 1
        export_loc = export_locations[0]
        save_path = f"{export_loc.save_loc}/{file_name}"
        duckdb_copy_to_file(
            to_copy=table, save_path=save_path, pg_url=pg_url, s3_cfg=export_loc.s3_cfg
        )
