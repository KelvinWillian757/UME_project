CREATE OR REPLACE TABLE `assertiv.staging.ml_features_credit_evaluations`
PARTITION BY DATE(update_timestamp)
CLUSTER BY issuer_id, store_state, status AS
SELECT
  evaluation_id,
  document,
  issuer_id,
  status,
  creation_timestamp,
  update_timestamp,
  borrower_cpf,
  borrower_id,
  store_city,
  store_state,
  retailer_id,
  retailer_issuer_id,
  retailer_category_id,
  store_id,
  bvs_score,
  conventional_score,
  decision_approved,
  CASE
    WHEN decision_approved = 'S' THEN 1
    WHEN decision_approved = 'N' THEN 0
    ELSE NULL
  END AS target_approved,
  TIMESTAMP_DIFF(evaluation_end_ts, evaluation_start_ts, MILLISECOND) AS evaluation_duration_ms,
  EXTRACT(HOUR FROM update_timestamp) AS event_hour,
  EXTRACT(DAYOFWEEK FROM update_timestamp) AS day_of_week,
  EXTRACT(MONTH FROM update_timestamp) AS event_month,
  ingested_at
FROM `assertiv.staging.stg_credit_evaluations`;

CREATE OR REPLACE TABLE `assertiv.staging.ml_training_credit_evaluations` AS
SELECT *
FROM `assertiv.staging.ml_features_credit_evaluations`
WHERE target_approved IS NOT NULL;

CREATE OR REPLACE TABLE `assertiv.staging.ml_online_credit_features`
PARTITION BY DATE(update_timestamp)
CLUSTER BY issuer_id, document AS
SELECT
  evaluation_id,
  document,
  issuer_id,
  store_city,
  store_state,
  retailer_id,
  retailer_category_id,
  bvs_score,
  conventional_score,
  event_hour,
  day_of_week,
  event_month,
  update_timestamp
FROM `assertiv.staging.ml_features_credit_evaluations`;
