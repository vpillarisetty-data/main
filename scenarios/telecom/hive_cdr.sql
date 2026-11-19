-- Legacy Telstra-style Hadoop table (Hive/Impala on HDFS) being migrated to Delta Lake.
CREATE EXTERNAL TABLE IF NOT EXISTS cdr.voice_cdr (
  cdr_id      STRING COMMENT 'switch record id',
  msisdn      STRING,
  imsi        STRING,
  cell_id     CHAR(8),
  call_type   VARCHAR(8),
  start_ts    TIMESTAMP,
  duration_s  INT,
  data_mb     DECIMAL(12,3)
)
PARTITIONED BY (event_date STRING, event_hour INT)
STORED AS ORC
LOCATION 'hdfs://nameservice1/warehouse/cdr/voice_cdr';
