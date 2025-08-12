-- trust_eval.model_business_metrics_daily
CREATE TABLE IF NOT EXISTS trust_eval.model_business_metrics_daily (
  model_name           STRING COMMENT 'Internal model key',
  model_version        STRING COMMENT 'Model version or hash',
  metric_id            STRING COMMENT 'Metric identifier from config',
  metric_value         DOUBLE COMMENT 'Computed metric value',
  numerator            DOUBLE COMMENT 'Optional numerator',
  denominator          DOUBLE COMMENT 'Optional denominator/reference',
  ds                   STRING COMMENT 'Partition date (UTC)',
  ts_computed_at       TIMESTAMP COMMENT 'Computation timestamp',
  config_hash          STRING COMMENT 'Hash of full config payload for lineage'
)
PARTITIONED BY (ds)
STORED AS PARQUET;

-- trust_eval.metric_audit_log
CREATE TABLE IF NOT EXISTS trust_eval.metric_audit_log (
  model_name     STRING,
  model_version  STRING,
  metric_id      STRING,
  ds             STRING,
  config_hash    STRING,
  ts_computed_at TIMESTAMP,
  status         STRING COMMENT 'ok|warn|page|error',
  error_msg      STRING
)
PARTITIONED BY (ds)
STORED AS PARQUET;
