# README

`data/iceberg/snowflake_base_parquet/data_sf0.01.parquet` is the file that was used to generate the data in `data/iceberg/snowflake`. 
All 5 queries from `scripts/test_data_generator/updates/q*.sql` have been applied to the table in snowflake.

`data/iceberg/snowflake_base_parquet/data_sf0.01_after.parquet` is this same date loaded in duckdb, have the same 5 queries applied, 
then exported to parquet

