CREATE OR REPLACE TABLE `assertiv.staging.raw_credit_evaluations_events` (
  event_id STRING,
  entity_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP,
  payload STRING,
  published_at TIMESTAMP
)
PARTITION BY DATE(published_at)
CLUSTER BY event_type, entity_id;

CREATE OR REPLACE TABLE `assertiv.staging.stg_credit_evaluations` (
  evaluation_id STRING,
  document STRING,
  issuer_id STRING,
  status STRING,
  creation_timestamp TIMESTAMP,
  update_timestamp TIMESTAMP,
  borrower_cpf STRING,
  borrower_id STRING,
  store_city STRING,
  store_state STRING,
  retailer_id STRING,
  retailer_issuer_id STRING,
  retailer_category_id STRING,
  store_id STRING,
  bvs_score INT64,
  bvs_status STRING,
  bvs_reason STRING,
  bvs_timestamp TIMESTAMP,
  decision_approved STRING,
  decision_text STRING,
  decision_score_code STRING,
  conventional_score FLOAT64,
  evaluation_start_ts TIMESTAMP,
  evaluation_end_ts TIMESTAMP,
  source_uuid STRING,
  source_timestamp INT64,
  ingested_at TIMESTAMP
)
PARTITION BY DATE(update_timestamp)
CLUSTER BY issuer_id, document, status;
