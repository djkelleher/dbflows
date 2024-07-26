## A Python/CLI tool for:
1. [Exporting](#export) database tables to compressed CSV files.
2. [Transferring](./dbflows/transfer.py) tables from from one database server to another.
3. [Loading](#loadingimporting) database data (from both files and Python)
4. [Creating/Managing](./dbflows/components) Postgresql/TimescaleDB tables, views, materialized views, functions, procedures, continuous aggregates, scheduled tasks.
5. [Checking](./dbflows/compare.py) for mismatched attributes between SQLAlchemy tables/models and actual tables in a database.

Currently only Postgresql and Postgresql-based databases (e.g. TimescaleDB) are supported.

## Install
`pip install dbflows`     

If using the export functionality (export database tables to compressed CSV files), then you will additionally need to have the `psql` executable available.
To install `psql`:
```bash
# enable PostgreSQL package repository
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo tee /etc/apt/trusted.gpg.d/pgdg.asc &>/dev/null
# replace `16` with the major version of your database
sudo apt update && sudo apt install -y postgresql-client-16
```

## Export
Features:
- File splitting. Create separate export files based on a 'slice column' (an orderable column. e.g. datetime, integer, etc) and/or 'partition column' (a categorical column. e.g. name string).
- Incremental exports (export only data not yet exported). This works for both single file and multiple/split file output.

### Examples
```py
from dbflows import export
import sqlalchemy as sa
# the table to export data from
my_table = sa.Table(
    "my_table", 
    sa.MetaData(schema="my_schema"), 
    sa.Column("inserted", sa.DateTime),
    sa.Column("category", sa.String),
    sa.Column("value", sa.Float),
)
# one or more save locations (2 in this case)
save_locs = ["s3://my-bucket/my_table_exports", "/path/to/local_dir/my_table_exports"]
# database URL
url = "postgres://user:password@hostname:port/database-name"
```
#### Export entire table to a single file.
```py
export(
    table=my_table,
    engine=url, # or sa.engine
    save_locs=save_locs
)
```
CLI equivalent:
```bash
db export table \
my_table.my_schema \
postgres://user:password@hostname:port/database-name` \
s3://my-bucket/my_table_exports \
/path/to/local_dir/my_table_exports
```

#### Export 500 MB CSVs, sorted and sliced on `inserted` datetime column.
```py
export(
    table=my_table,
    engine=url, # or sa.engine
    save_locs=save_locs,
    slice_column=my_table.c.inserted,
    file_max_size="500 MB"
)
```

#### Create a CSV export for each unique category in table.
```py
export(
    table=my_table,
    engine=url, # or sa.engine
    save_locs=save_locs,
    partition_column=my_table.c.category
)
```
CLI equivalent:
```bash
db export table \
my_table.my_schema \
postgres://user:password@hostname:port/database-name` \
# save to one or more locations (s3 paths or local)
s3://my-bucket/my_table_exports \ 
/path/to/local_dir/my_table_exports \ 
--partition-column category # or "-p category"
```

#### export 500 MB CSVs for each unique category, sorted and sliced on `inserted` datetime column.
```py
export(
    table=my_table,
    engine=url, # or sa.engine
    save_locs=save_locs,
    slice_column=my_table.c.inserted,
    file_max_size="500 MB",
    partition_column=my_table.c.category,
)
```

# Loading/Importing
### Loading from Python objects
Create a [`PgLoader`](./dbflows/load.py#L30) instance for your table and use the [`load`](./dbflows/load.py#L175) method to load batches of rows.

### Loading from CSV files
Use [import_csvs](./dbflows/files.py#L10) to load CSV with parallel worker threads. This internally uses [timescaledb-parallel-copy](https://docs.timescale.com/use-timescale/latest/ingest-data/about-timescaledb-parallel-copy/) which can be installed with: `go install github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy@latest`