from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal, Optional, Sequence, Union

import sqlalchemy as sa

from dbflows.utils import schema_table, split_schema_table
from fileflows import Files
from fileflows.s3 import S3Cfg, is_s3_path

from .utils import duckdb_copy_to_file


@dataclass
class ExportLocation:
    """Location to save export data file to."""

    save_path: Union[Path, str]
    # S3 credentials (if save_loc is an S3  path)
    s3_cfg: Optional[S3Cfg] = None

    def __post_init__(self):
        self.save_loc = str(self.save_loc)
        self.files = Files(s3_cfg=self.s3_cfg)

    @classmethod
    def from_table_and_type(
        cls,
        table: str,
        file_type: Literal["csv", "csv.gz", "parquet"],
        save_loc: Union[Path, str],
        s3_cfg: Optional[S3Cfg] = None,
    ):
        file_name = "/".join(split_schema_table(schema_table(table))) + f".{file_type}"
        return cls(save_path=f"{save_loc}/{file_name}", s3_cfg=s3_cfg)

    @property
    def is_s3(self) -> bool:
        return is_s3_path(self.save_path)

    def create(self):
        """make sure bucket or local directory exists."""
        self.files.create(self.save_path)


def export_table(
    table: Union[str, sa.Table],
    pg_url: str,
    export_locations: Sequence[Union[str, ExportLocation]],
):
    """Export a table to a file."""
    if not isinstance(export_locations, (list, tuple)):
        export_locations = [export_locations]
    export_locations = [
        ExportLocation(loc) if isinstance(loc, str) else loc for loc in export_locations
    ]
    # run for each file type (if multiple)
    file_type_locations = defaultdict(list)
    for loc in export_locations:
        loc.create()
        file_type_locations[loc.save_path.suffix].append(loc)
    for suffix, locations in file_type_locations.items():
        # check if any save location is a local directory.
        local_dirs = [l for l in locations if not l.is_s3]
        if local_dirs:
            export_loc = local_dirs.pop(0)
            duckdb_copy_to_file(
                to_copy=table,
                save_path=export_loc.save_path,
                pg_url=pg_url,
                s3_cfg=export_loc.s3_cfg,
            )
            # copy to other locations.
            s3_dirs = [l for l in export_locations if l.is_s3]
            for loc in local_dirs + s3_dirs:
                loc.files.copy(export_loc.save_path, loc.save_path)
        elif len(export_locations) > 1:
            with NamedTemporaryFile() as f:
                save_path = f.name + suffix
                duckdb_copy_to_file(to_copy=table, save_path=save_path, pg_url=pg_url)
                # copy to all destinations
                for loc in export_locations:
                    loc.files.copy(save_path, loc.save_path)
        else:
            assert len(export_locations) == 1
            export_loc = export_locations[0]
            duckdb_copy_to_file(
                to_copy=table,
                save_path=export_loc.save_path,
                pg_url=pg_url,
                s3_cfg=export_loc.s3_cfg,
            )
