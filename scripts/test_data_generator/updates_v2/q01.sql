update iceberg_catalog.pyspark_iceberg_table
set l_orderkey_bool=NULL,
    l_partkey_int=NULL,
    l_suppkey_long=NULL,
    l_extendedprice_float=NULL,
    l_extendedprice_double=NULL,
    l_shipdate_date=NULL,
    l_partkey_time=NULL,
    l_commitdate_timestamp=NULL,
    l_commitdate_timestamp_tz=NULL,
    l_comment_string=NULL,
    l_comment_blob=NULL
where l_partkey_int % 2 = 0;