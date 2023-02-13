CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorreadingtime` bigint, 
  `serialnumber` string, 
  `distancefromobject` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://avishni-udmde/step_trainer_trusted'
TBLPROPERTIES (
  'classification'='parquet', 
  'transient_lastDdlTime'='1675940033')