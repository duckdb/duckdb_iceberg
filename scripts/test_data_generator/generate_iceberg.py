#!/usr/bin/python3
import pyspark
import pyspark.sql
import sys
import duckdb
import os
from pyspark import SparkContext
from pathlib import Path

if (len(sys.argv) != 4 ):
    print("Usage: generate_iceberg.py <SCALE_FACTOR> <DEST_PATH> <ICBERG_SPEC_VERSION>")
    exit(1)

SCALE = sys.argv[1]
DEST_PATH = sys.argv[2]
ICEBERG_SPEC_VERSION = sys.argv[3]

PARQUET_SRC_FILE = f'{DEST_PATH}/base_file/file.parquet'
TABLE_NAME = "iceberg_catalog.pyspark_iceberg_table";
CWD = os.getcwd()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

###
### Generate dataset
###
os.system(f"python3 {SCRIPT_DIR}/generate_base_parquet.py {SCALE} {CWD}/{DEST_PATH} spark")

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
conf.set('spark.jars', f'{SCRIPT_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar')
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

###
### Create Iceberg table from dataset
###
spark.read.parquet(PARQUET_SRC_FILE).createOrReplaceTempView('parquet_file_view');

if ICEBERG_SPEC_VERSION == '1':
    spark.sql(f"CREATE or REPLACE TABLE {TABLE_NAME} TBLPROPERTIES ('format-version'='{ICEBERG_SPEC_VERSION}') AS SELECT * FROM parquet_file_view");
elif ICEBERG_SPEC_VERSION == '2':
    spark.sql(f"CREATE or REPLACE TABLE {TABLE_NAME} TBLPROPERTIES ('format-version'='{ICEBERG_SPEC_VERSION}', 'write.update.mode'='merge-on-read') AS SELECT * FROM parquet_file_view");
else:
    print(f"Are you from the future? Iceberg spec version '{ICEBERG_SPEC_VERSION}' is unbeknownst to me")
    exit(1)

###
### Apply modifications to base table generating verification results between each step
###
update_files = [str(path) for path in Path(f'{SCRIPT_DIR}/updates_v{ICEBERG_SPEC_VERSION}').rglob('*.sql')]
update_files.sort() # Order matters obviously
last_file = ""

for path in update_files:
    full_file_path = f"{SCRIPT_DIR}/updates_v{ICEBERG_SPEC_VERSION}/{os.path.basename(path)}"
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
