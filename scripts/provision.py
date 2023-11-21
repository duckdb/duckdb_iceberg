# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql import SparkSession

import os

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 pyspark-shell"
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

spark = (
    SparkSession.builder.appName("DuckDB REST Integeration test")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.type", "rest")
    .config("spark.sql.catalog.demo.uri", "http://127.0.0.1:8181")
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/")
    .config("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000")
    .config("spark.sql.catalog.demo.s3.path-style-access", "true")
    .config("spark.sql.defaultCatalog", "demo")
    .config("spark.sql.catalogImplementation", "in-memory")
    .getOrCreate()
)

spark.sql(
    """
  CREATE DATABASE IF NOT EXISTS default;
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE default.table_unpartitioned (
    dt     date,
    number integer,
    letter string
)
USING iceberg
"""
)

spark.sql(
    """
        INSERT INTO default.table_unpartitioned
        VALUES
            (CAST('2023-03-01' AS date), 1, 'a'),
            (CAST('2023-03-02' AS date), 2, 'b'),
            (CAST('2023-03-03' AS date), 3, 'c'),
            (CAST('2023-03-04' AS date), 4, 'd'),
            (CAST('2023-03-05' AS date), 5, 'e'),
            (CAST('2023-03-06' AS date), 6, 'f'),
            (CAST('2023-03-07' AS date), 7, 'g'),
            (CAST('2023-03-08' AS date), 8, 'h'),
            (CAST('2023-03-09' AS date), 9, 'i'),
            (CAST('2023-03-10' AS date), 10, 'j'),
            (CAST('2023-03-11' AS date), 11, 'k'),
            (CAST('2023-03-12' AS date), 12, 'l');
    """
)


spark.sql(
    """
CREATE OR REPLACE TABLE default.table_partitioned (
    dt     date,
    number integer,
    letter string
)
USING iceberg
PARTITIONED BY (days(dt))
"""
)

spark.sql(
    """
        INSERT INTO default.table_partitioned
        VALUES
            (CAST('2023-03-01' AS date), 1, 'a'),
            (CAST('2023-03-02' AS date), 2, 'b'),
            (CAST('2023-03-03' AS date), 3, 'c'),
            (CAST('2023-03-04' AS date), 4, 'd'),
            (CAST('2023-03-05' AS date), 5, 'e'),
            (CAST('2023-03-06' AS date), 6, 'f'),
            (CAST('2023-03-07' AS date), 7, 'g'),
            (CAST('2023-03-08' AS date), 8, 'h'),
            (CAST('2023-03-09' AS date), 9, 'i'),
            (CAST('2023-03-10' AS date), 10, 'j'),
            (CAST('2023-03-11' AS date), 11, 'k'),
            (CAST('2023-03-12' AS date), 12, 'l');
    """
)

# By default, Spark uses merge on write deletes
# which optimize for read-performance

spark.sql(
    """
CREATE OR REPLACE TABLE default.table_mor_deletes (
    dt     date,
    number integer,
    letter string
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
"""
)


spark.sql(
    """
        INSERT INTO default.table_mor_deletes
        VALUES
            (CAST('2023-03-01' AS date), 1, 'a'),
            (CAST('2023-03-02' AS date), 2, 'b'),
            (CAST('2023-03-03' AS date), 3, 'c'),
            (CAST('2023-03-04' AS date), 4, 'd'),
            (CAST('2023-03-05' AS date), 5, 'e'),
            (CAST('2023-03-06' AS date), 6, 'f'),
            (CAST('2023-03-07' AS date), 7, 'g'),
            (CAST('2023-03-08' AS date), 8, 'h'),
            (CAST('2023-03-09' AS date), 9, 'i'),
            (CAST('2023-03-10' AS date), 10, 'j'),
            (CAST('2023-03-11' AS date), 11, 'k'),
            (CAST('2023-03-12' AS date), 12, 'l');
    """
)
