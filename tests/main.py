import pytest

args = [
    "-s",
    "-vvv",
    "--pg-url",
    "postgresql://test:test@postgres:5432/test",
    "--minio-endpoint",
    "http://minio:9000",
    "--minio-user",
    "minioadmin",
    "--minio-password",
    "minioadmin",
]

files = [
    # "test_compare.py",
    #"test_copy.py",
    #"test_file_meta.py",
    #"test_export_table.py",
    #"test_export_partitions.py",
    #"test_export_slices.py",
    #"test_export_partition_slices.py",
    #"test_loader_filters.py",
    #"test_loader.py",
    "test_transfer.py",
]
if __name__ == "__main__":
    # while True:
    for file in files:
        pytest.main([f"/opt/dbflows/tests/{file}"] + args)
