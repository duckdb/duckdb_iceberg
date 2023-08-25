update iceberg_catalog.pyspark_iceberg_table
set l_orderkey_bool = false
where l_partkey_int % 5 = 0;