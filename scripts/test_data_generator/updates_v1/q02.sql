insert into iceberg_catalog.pyspark_iceberg_table
select * FROM iceberg_catalog.pyspark_iceberg_table
where l_extendedprice_double < 30000