CREATE OR REPLACE TABLE `assertiv.staging.quarantine_credit_evaluations_events` (
  event_id STRING,
  entity_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP,
  payload STRING,
  error_stage STRING,
  error_message STRING,
  quarantined_at TIMESTAMP
)
PARTITION BY DATE(quarantined_at)
CLUSTER BY error_stage, entity_id;

CREATE OR REPLACE TABLE `assertiv.staging.audit_credit_evaluations_pipeline` (
  audit_id STRING,
  event_id STRING,
  entity_id STRING,
  event_type STRING,
  status STRING,
  stage STRING,
  message STRING,
  created_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY status, stage, entity_id;
