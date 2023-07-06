Warning: This extension currently builds with a feature branch of DuckDB. A PR is being worked on. When the PR is merged,
this extension will be updated and usable from (nightly) DuckDB releases.

# DuckDB extension for Apache Iceberg 
**Disclaimer:** This extension is currently in an experimental state. Feel free to try it out, but be aware that minimal testing and
benchmarking was done.

This repository contains a DuckDB extension that adds support for [Apache Iceberg](https://iceberg.apache.org/). In its current state, the extension offers some basics features that allow listing snapshots and reading specific snapshots
of an iceberg tables.

# Acknowledgments
This extension was initially developed as part of a customer project for [RelationalAI](https://relational.ai/),
who have agreed to open source the extension. We would like to thank RelationalAI for their support
and their commitment to open source enabling us to share this extension with the community.

# Dependencies
## building
This extension has several dependencies. Currently, the main way to install them is through vcpkg. To install vcpkg, 
check out the docs [here](https://vcpkg.io/en/getting-started.html). Note that this extension contains a custom vcpkg port
that overrides the existing 'avro-cpp' port of vcpkg. The reason for this is that the other versions of avro-cpp have
some issue that seems to cause issues with the avro files produced by the spark iceberg extension.

## test data generation
To generate test data, the script in 'scripts/test_data_generator' is used to have spark generate some test data. This is 
based on pyspark 3.4, which you can install through pip. 

# Building the extension
To build the extension with vcpkg, you can build this extension using:

```shell
VCPKG_TOOLCHAIN_PATH='<path_to_your_vcpkg_toolchain>' make
```

This will build both the separate loadable extension and a duckdb binary with the extension pre-loaded:
```shell
./build/release/duckdb
./build/release/extension/iceberg/iceberg.duckdb_extension
```

# Running iceberg queries
The easiest way is to start the duckdb binary produced by the build step: `./build/release/duckdb`. Then for example:
```SQL
> SELECT count(*) FROM ICEBERG_SCAN('data/iceberg/lineitem_iceberg', ALLOW_MOVED_PATHS=TRUE);
51793
```
Note that for testing, the `ALLOW_MOVED_PATHS` option is available. This option will ensure some path resolution is performed. This
path resolution allows scanning iceberg tables that are moved, which is used during testing.

```SQL
> SELECT * FROM ICEBERG_SNAPSHOTS('data/iceberg/lineitem_iceberg', ALLOW_MOVED_PATHS=TRUE);
1	3776207205136740581	2023-02-15 15:07:54.504	0	lineitem_iceberg/metadata/snap-3776207205136740581-1-cf3d0be5-cf70-453d-ad8f-48fdc412e608.avro
2	7635660646343998149	2023-02-15 15:08:14.73	0	lineitem_iceberg/metadata/snap-7635660646343998149-1-10eaca8a-1e1c-421e-ad6d-b232e5ee23d3.avro
```
For more examples check the tests in the `test` directory

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