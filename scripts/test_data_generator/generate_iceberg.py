#!/usr/bin/python3
import pyspark.sql
import sys
import duckdb
import os
from pyspark import SparkContext
from pathlib import Path

if (len(sys.argv) != 3):
    print("Usage: generate_iceberg.py <SCALE_FACTOR> <DEST_PATH>")
    exit(1)

SCALE = sys.argv[1]
DEST_PATH = sys.argv[2]
PARQUET_SRC_FILE = f'{DEST_PATH}/base_file/file.parquet'
TABLE_NAME = "iceberg_catalog.pyspark_iceberg_table";
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

# TODO: TIME type
# ALSO COOL: UUID not supported in spark https://github.com/apache/iceberg/issues/4038, is a STRING now
con.query("""CREATE VIEW test_table as
                SELECT
                (l_orderkey%2=0) as l_orderkey_bool,
                l_partkey::INT32 as l_partkey_int,
                l_suppkey::INT64 as l_suppkey_long,
                l_extendedprice::FLOAT as l_extendedprice_float,
                l_extendedprice::DOUBLE as l_extendedprice_double,
                l_shipdate::DATE as l_shipdate_date,
                l_partkey as l_partkey_time,
                l_commitdate::TIMESTAMP as l_commitdate_timestamp,
                l_commitdate::TIMESTAMPTZ as l_commitdate_timestamp_tz,
                l_comment as l_comment_string,
                gen_random_uuid()::VARCHAR as uuid,
                l_comment::BLOB as l_comment_blob,
                {'a': l_shipmode, 'b': l_quantity} as l_shipmode_quantity_struct,
                [l_linenumber, l_quantity] as l_linenumber_quantity_list,
                map(['linenumber', 'quantity'], [l_linenumber, l_quantity]) as l_linenumber_quantity_map
                FROM
                lineitem;""");

os.makedirs(os.path.dirname(PARQUET_SRC_FILE), exist_ok=True)
con.query(f"COPY test_table TO '{PARQUET_SRC_FILE}'");

###
### Configure everyone's favorite apache product
###
conf = pyspark.SparkConf()
conf.setMaster('local[*]')
conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
conf.set('spark.sql.catalog.iceberg_catalog.warehouse', DEST_PATH)
conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
conf.set('spark.driver.memory', '10g')
conf.set('spark.jars', f'{SCRIPT_DIR}/iceberg-spark-runtime-3.3_2.12-1.0.0.jar')
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

###
### Create Iceberg table from dataset
###
spark.read.parquet(PARQUET_SRC_FILE).createOrReplaceTempView('parquet_file_view');
spark.sql(f"CREATE or REPLACE TABLE {TABLE_NAME} TBLPROPERTIES ('format-version'='2', 'write.update.mode'='merge-on-read') AS SELECT * FROM parquet_file_view");

###
### Apply modifications to base table generating verification results between each step
###
update_files = [str(path) for path in Path(f'{SCRIPT_DIR}').rglob('*.sql')]
update_files.sort() # Order matters obviously
last_file = ""

for path in update_files:
    full_file_path = f"{SCRIPT_DIR}/updates/{os.path.basename(path)}"
    with open(full_file_path, 'r') as file:
        file_trimmed = os.path.basename(path)[:-4]
        last_file = file_trimmed
        print(f"Applying {file_trimmed} to DB")
        query = file.read()
        # Run spark query
        spark.sql(query)
        print(f"Writing verification data")

        # Write total count
        ret = spark.sql(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        out_path = f'{DEST_PATH}/expected_results/{file_trimmed}/count.csv'
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, 'w') as f:
            f.write("count\n")
            f.write('%d' % ret.collect()[0][0])

        # Create copy of table
        df = spark.read.table(TABLE_NAME)
        df.write.parquet(f"{DEST_PATH}/expected_results/{file_trimmed}/data");

        # For documentation, also write the query we executed to the data
        query_path = f'{DEST_PATH}/expected_results/{file_trimmed}/query.sql'
        with open(query_path, 'w') as f:
            f.write("-- The query executed at this step:\n")
            f.write(query)


###
### Finally, we copy the latest results to a "final" dir for easy test writing
###
import shutil
shutil.copytree(f"{DEST_PATH}/expected_results/{last_file}", f"{DEST_PATH}/expected_results/last")