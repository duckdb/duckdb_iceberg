#!/usr/bin/python3
import pyspark.sql
import sys
import duckdb
import os
from pyspark import SparkContext
from pathlib import Path

if (len(sys.argv) < 3):
    print("Usage: generate_base_parquet.py <SCALE_FACTOR> <DEST_PATH> (<MODE>)")
    exit(1)

SCALE = sys.argv[1]
DEST_PATH = sys.argv[2]
MODE = sys.argv[3] if len(sys.argv) > 3 else "default";
PARQUET_SRC_FILE = f'{DEST_PATH}/base_file/file.parquet'
CWD = os.getcwd()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

###
### Generate dataset from DuckDB
###
con = duckdb.connect()
con.query("INSTALL tpch")
con.query("LOAD tpch")
con.query(f"SELECT setseed(0.42);")
con.query(f"CALL dbgen(sf={SCALE});")

# TODO: we're missing the fixed byte array, as we don't have one in duckdb

# UUID not supported in spark https://github.com/apache/iceberg/issues/4038, is a STRING now
if (MODE.lower() == "spark"):
    con.query("""CREATE VIEW test_table as
                    SELECT
                    (l_orderkey%2=0) as l_orderkey_bool,
                    l_partkey::INT32 as l_partkey_int,
                    l_suppkey::INT64 as l_suppkey_long,
                    l_extendedprice::FLOAT as l_extendedprice_float,
                    l_extendedprice::DOUBLE as l_extendedprice_double,
                    l_extendedprice::DECIMAL(9,2) as l_extendedprice_dec9_2,
                    l_extendedprice::DECIMAL(18,6) as l_extendedprice_dec18_6,
                    l_extendedprice::DECIMAL(38,10) as l_extendedprice_dec38_10,
                    l_shipdate::DATE as l_shipdate_date,
                    l_partkey as l_partkey_time,
                    l_commitdate::TIMESTAMP as l_commitdate_timestamp,
                    l_commitdate::TIMESTAMPTZ as l_commitdate_timestamp_tz,
                    l_comment as l_comment_string,
                    gen_random_uuid()::VARCHAR as uuid,
                    l_comment::BLOB as l_comment_blob
                    FROM
                    lineitem;""");
elif (MODE.lower() == "default"):
    con.query("""CREATE VIEW test_table as
                    SELECT
                    (l_orderkey%2=0) as l_orderkey_bool,
                    l_partkey::INT32 as l_partkey_int,
                    l_suppkey::INT64 as l_suppkey_long,
                    l_extendedprice::FLOAT as l_extendedprice_float,
                    l_extendedprice::DOUBLE as l_extendedprice_double,
                    l_extendedprice::DECIMAL(9,2) as l_extendedprice_dec9_2,
                    l_extendedprice::DECIMAL(18,6) as l_extendedprice_dec18_6,
                    l_extendedprice::DECIMAL(38,10) as l_extendedprice_dec38_10,
                    l_shipdate::DATE as l_shipdate_date,
                    make_time(l_partkey%24, l_partkey%60, 0) as l_partkey_time,
                    make_time(l_partkey%24, l_partkey%60, 0)::TIMETZ as l_partkey_time_tz,
                    l_commitdate::TIMESTAMP as l_commitdate_timestamp,
                    l_commitdate::TIMESTAMPTZ as l_commitdate_timestamp_tz,
                    l_comment as l_comment_string,
                    gen_random_uuid()::UUID as uuid,
                    l_comment::BLOB as l_comment_blob
                    FROM
                    lineitem;""");
else:
    print(f"Unknown mode '{MODE}'")
    exit(1)

os.makedirs(os.path.dirname(PARQUET_SRC_FILE), exist_ok=True)
con.query(f"COPY test_table TO '{PARQUET_SRC_FILE}'");

