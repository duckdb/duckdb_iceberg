# DuckDB Iceberg extension
Extension for DuckDB to read from Apache Iceberg.

# Examples
```SQL
> SELECT count(*) FROM ICEBERG_SCAN('data/iceberg/lineitem_iceberg', allow_moved_paths=TRUE);
51793
```

```SQL
> SELECT * FROM ICEBERG_SNAPSHOTS('data/iceberg/lineitem_iceberg');
1	3776207205136740581	2023-02-15 15:07:54.504	0	lineitem_iceberg/metadata/snap-3776207205136740581-1-cf3d0be5-cf70-453d-ad8f-48fdc412e608.avro
2	7635660646343998149	2023-02-15 15:08:14.73	0	lineitem_iceberg/metadata/snap-7635660646343998149-1-10eaca8a-1e1c-421e-ad6d-b232e5ee23d3.avro
```

# Building
To build the extension, install the dependencies and run
```shell
make
```
This will build both the separate loadable extension and a duckdb binary with the extension pre-loaded:
```shell
./build/release/duckdb
./build/release/extension/iceberg/iceberg.duckdb_extension
```

# Dependencies
Currently building the extension requires Boost as the Avro C++ library needs it. Additionally, if the extension was built on a system with Snappy, it also requires
Snappy to run the extension. To install, run these commands (or something similar depending on platform):
- Linux: `apt-get install libsnappy-dev libboost-dev`
- MacOS: `brew install boost snappy`

# Running tests
## Generating test data
To generate the test data, run:
```shell
make data
```
Note that the script requires python3, pyspark and duckdb-python to be installed. Assuming python3 is already installed,
running `python3 -m pip install duckdb pyspark` should do the trick.

## Running unittests
```shell
make test 
```

## Running the local S3 test server
Running the S3 test cases requires the minio test server to be running and populated with `scripts/upload_iceberg_to_s3_test_server.sh`.
Note that this requires to have run `make data` before and also to have the aws cli and docker compose installed.