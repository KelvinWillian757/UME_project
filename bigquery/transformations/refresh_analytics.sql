CREATE OR REPLACE TABLE `assertiv.staging.analytics_credit_evaluations`
PARTITION BY DATE(update_timestamp)
CLUSTER BY issuer_id, status, store_state AS
SELECT
  *,
  TIMESTAMP_DIFF(evaluation_end_ts, evaluation_start_ts, MILLISECOND) AS evaluation_duration_ms
FROM `assertiv.staging.stg_credit_evaluations`;

CREATE OR REPLACE TABLE `assertiv.staging.analytics_credit_evaluations_daily` AS
SELECT
  DATE(update_timestamp) AS reference_date,
  issuer_id,
  store_state,
  store_city,
  status,
  COUNT(*) AS total_evaluations,
  COUNTIF(status = 'APPROVED') AS total_approved,
  COUNTIF(status = 'DENIED') AS total_denied,
  AVG(bvs_score) AS avg_bvs_score,
  AVG(conventional_score) AS avg_conventional_score,
  AVG(TIMESTAMP_DIFF(evaluation_end_ts, evaluation_start_ts, MILLISECOND)) AS avg_duration_ms
FROM `assertiv.staging.stg_credit_evaluations`
GROUP BY 1,2,3,4,5;

CREATE OR REPLACE TABLE `assertiv.staging.analytics_credit_kpis` AS
SELECT
  CURRENT_TIMESTAMP() AS generated_at,
  COUNT(*) AS total_evaluations,
  COUNTIF(status = 'APPROVED') AS total_approved,
  COUNTIF(status = 'DENIED') AS total_denied,
  SAFE_DIVIDE(COUNTIF(status = 'APPROVED'), COUNT(*)) AS approval_rate,
  AVG(bvs_score) AS avg_bvs_score,
  AVG(conventional_score) AS avg_conventional_score,
  AVG(TIMESTAMP_DIFF(evaluation_end_ts, evaluation_start_ts, MILLISECOND)) AS avg_duration_ms
FROM `assertiv.staging.stg_credit_evaluations`;
