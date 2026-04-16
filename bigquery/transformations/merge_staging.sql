MERGE `assertiv.staging.stg_credit_evaluations` AS T
USING (
  WITH base AS (
    SELECT
      JSON_VALUE(payload, '$.payload.id') AS evaluation_id,
      JSON_VALUE(payload, '$.payload.document') AS document,
      JSON_VALUE(payload, '$.payload.issuer_id') AS issuer_id,
      JSON_VALUE(payload, '$.payload.status') AS status,
      SAFE_CAST(JSON_VALUE(payload, '$.payload.creation_timestamp') AS TIMESTAMP) AS creation_timestamp,
      SAFE_CAST(JSON_VALUE(payload, '$.payload.update_timestamp') AS TIMESTAMP) AS update_timestamp,
      JSON_VALUE(payload, '$.payload.evaluation_request.borrower.cpf') AS borrower_cpf,
      JSON_VALUE(payload, '$.payload.evaluation_request.borrower.id') AS borrower_id,
      JSON_VALUE(payload, '$.payload.evaluation_request.store.address.city') AS store_city,
      JSON_VALUE(payload, '$.payload.evaluation_request.store.address.federativeUnit') AS store_state,
      JSON_VALUE(payload, '$.payload.evaluation_request.retailer.id') AS retailer_id,
      JSON_VALUE(payload, '$.payload.evaluation_request.retailer.issuerId') AS retailer_issuer_id,
      JSON_VALUE(payload, '$.payload.evaluation_request.retailer.categories[0].id') AS retailer_category_id,
      JSON_VALUE(payload, '$.payload.evaluation_request.store.id') AS store_id,
      SAFE_CAST(JSON_VALUE(payload, '$.payload.evaluation_response.decisions.bvs.data.score') AS INT64) AS bvs_score,
      JSON_VALUE(payload, '$.payload.evaluation_response.decisions.bvs.status') AS bvs_status,
      JSON_VALUE(payload, '$.payload.evaluation_response.decisions.bvs.reason') AS bvs_reason,
      SAFE_CAST(JSON_VALUE(payload, '$.payload.evaluation_response.decisions.bvs.data.timestamp') AS TIMESTAMP) AS bvs_timestamp,
      JSON_VALUE(payload, '$.payload.evaluation_response.decisions.bvs.data.acertaCompletoPositivo.decisao.aprova') AS decision_approved,
      JSON_VALUE(payload, '$.payload.evaluation_response.decisions.bvs.data.acertaCompletoPositivo.decisao.texto') AS decision_text,
      JSON_VALUE(payload, '$.payload.evaluation_response.decisions.bvs.data.acertaCompletoPositivo.decisao.score') AS decision_score_code,
      SAFE_CAST(JSON_VALUE(payload, '$.payload.evaluation_response.decisions.conventionalApplication.data.score') AS FLOAT64) AS conventional_score,
      SAFE_CAST(JSON_VALUE(payload, '$.payload.evaluation_response.startTimestamp') AS TIMESTAMP) AS evaluation_start_ts,
      SAFE_CAST(JSON_VALUE(payload, '$.payload.evaluation_response.endTimestamp') AS TIMESTAMP) AS evaluation_end_ts,
      JSON_VALUE(payload, '$.payload.datastream_metadata.uuid') AS source_uuid,
      SAFE_CAST(JSON_VALUE(payload, '$.payload.datastream_metadata.source_timestamp') AS INT64) AS source_timestamp,
      CURRENT_TIMESTAMP() AS ingested_at
    FROM `assertiv.staging.raw_credit_evaluations_events`
  ),
  dedup AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY evaluation_id
      ORDER BY update_timestamp DESC, source_timestamp DESC
    ) = 1
  )
  SELECT * FROM dedup
) AS S
ON T.evaluation_id = S.evaluation_id
WHEN MATCHED AND (
  S.update_timestamp > T.update_timestamp
  OR (S.update_timestamp = T.update_timestamp AND S.source_timestamp > T.source_timestamp)
) THEN
UPDATE SET
  document = S.document,
  issuer_id = S.issuer_id,
  status = S.status,
  creation_timestamp = S.creation_timestamp,
  update_timestamp = S.update_timestamp,
  borrower_cpf = S.borrower_cpf,
  borrower_id = S.borrower_id,
  store_city = S.store_city,
  store_state = S.store_state,
  retailer_id = S.retailer_id,
  retailer_issuer_id = S.retailer_issuer_id,
  retailer_category_id = S.retailer_category_id,
  store_id = S.store_id,
  bvs_score = S.bvs_score,
  bvs_status = S.bvs_status,
  bvs_reason = S.bvs_reason,
  bvs_timestamp = S.bvs_timestamp,
  decision_approved = S.decision_approved,
  decision_text = S.decision_text,
  decision_score_code = S.decision_score_code,
  conventional_score = S.conventional_score,
  evaluation_start_ts = S.evaluation_start_ts,
  evaluation_end_ts = S.evaluation_end_ts,
  source_uuid = S.source_uuid,
  source_timestamp = S.source_timestamp,
  ingested_at = S.ingested_at
WHEN NOT MATCHED THEN
INSERT (
  evaluation_id, document, issuer_id, status, creation_timestamp, update_timestamp,
  borrower_cpf, borrower_id, store_city, store_state, retailer_id, retailer_issuer_id,
  retailer_category_id, store_id, bvs_score, bvs_status, bvs_reason, bvs_timestamp,
  decision_approved, decision_text, decision_score_code, conventional_score,
  evaluation_start_ts, evaluation_end_ts, source_uuid, source_timestamp, ingested_at
)
VALUES (
  S.evaluation_id, S.document, S.issuer_id, S.status, S.creation_timestamp, S.update_timestamp,
  S.borrower_cpf, S.borrower_id, S.store_city, S.store_state, S.retailer_id, S.retailer_issuer_id,
  S.retailer_category_id, S.store_id, S.bvs_score, S.bvs_status, S.bvs_reason, S.bvs_timestamp,
  S.decision_approved, S.decision_text, S.decision_score_code, S.conventional_score,
  S.evaluation_start_ts, S.evaluation_end_ts, S.source_uuid, S.source_timestamp, S.ingested_at
);
