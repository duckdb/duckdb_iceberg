# README
Script used to generate test data for this repo. Don't use directly, but use makefile recipe `make data`

# Valdation
- count(*) after each step
- full table copy to parquet file after each step

# Idea behind script:
- generated data easily within this repo
- contains all iceberg datatypes
- contains nulls
- configurable scale factor
- automatic verification

# Update queries
Should be portable between DuckDB, Spark and Snowflake

# Todo's:
- Arbitrary precision Decimals?
- Time not yet working
- PySpark does not support UUID