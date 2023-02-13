CREATE EXTERNAL TABLE `accelerometer_trusted1`(
  `timestamp` bigint, 
  `user` string, 
  `x` double, 
  `y` double, 
  `z` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://avishni-udmde/accelerometer_trusted'
TBLPROPERTIES (
  'classification'='parquet', 
  'transient_lastDdlTime'='1675796356')