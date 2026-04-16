import base64
import json
import uuid
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)

PROJECT_ID = "assertiv"

RAW_TABLE = "assertiv.staging.raw_credit_evaluations_events"
STG_TABLE = "assertiv.staging.stg_credit_evaluations"

ANALYTICS_TABLE = "assertiv.staging.analytics_credit_evaluations"
ANALYTICS_DAILY_TABLE = "assertiv.staging.analytics_credit_evaluations_daily"
ANALYTICS_KPIS_TABLE = "assertiv.staging.analytics_credit_kpis"

ML_FEATURES_TABLE = "assertiv.staging.ml_features_credit_evaluations"
ML_TRAINING_TABLE = "assertiv.staging.ml_training_credit_evaluations"
ML_ONLINE_TABLE = "assertiv.staging.ml_online_credit_features"

QUARANTINE_TABLE = "assertiv.staging.quarantine_credit_evaluations_events"
AUDIT_TABLE = "assertiv.staging.audit_credit_evaluations_pipeline"

bq_client = bigquery.Client(project=PROJECT_ID)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_row_id(*parts) -> str:
    """
    Gera um row_id estável para deduplicação best-effort no BigQuery streaming.
    """
    safe_parts = [str(p) if p is not None else "null" for p in parts]
    return "_".join(safe_parts)


def write_audit(event_id, entity_id, event_type, status, stage, message):
    row = {
        "audit_id": str(uuid.uuid4()),
        "event_id": str(event_id) if event_id is not None else None,
        "entity_id": entity_id,
        "event_type": event_type,
        "status": status,
        "stage": stage,
        "message": message,
        "created_at": iso_now()
    }

    row_id = build_row_id(event_id, entity_id, event_type, status, stage)

    errors = bq_client.insert_rows_json(
        AUDIT_TABLE,
        [row],
        row_ids=[row_id]
    )

    if errors:
        print(f"Erro ao gravar auditoria: {errors}")


def write_quarantine(event_id, entity_id, event_type, event_timestamp, payload, error_stage, error_message):
    row = {
        "event_id": str(event_id) if event_id is not None else None,
        "entity_id": entity_id,
        "event_type": event_type,
        "event_timestamp": event_timestamp,
        "payload": payload,
        "error_stage": error_stage,
        "error_message": error_message,
        "quarantined_at": iso_now()
    }

    row_id = build_row_id(event_id, entity_id, event_type, error_stage)

    errors = bq_client.insert_rows_json(
        QUARANTINE_TABLE,
        [row],
        row_ids=[row_id]
    )

    if errors:
        print(f"Erro ao gravar quarentena: {errors}")


def validate_payload(payload_json):
    required_root_fields = ["event_id", "entity_id", "event_type", "payload"]
    for field in required_root_fields:
        if field not in payload_json:
            return False, f"campo obrigatório ausente no envelope: {field}"

    business_payload = payload_json.get("payload", {})
    required_business_fields = ["id", "document", "issuer_id", "status"]
    for field in required_business_fields:
        if field not in business_payload:
            return False, f"campo obrigatório ausente no payload: {field}"

    return True, "ok"


def run_merge_staging():
    sql = f"""
    MERGE `{STG_TABLE}` AS T
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
        FROM `{RAW_TABLE}`
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
      )
    """
    bq_client.query(sql).result()


def refresh_analytics():
    sql_1 = f"""
    CREATE OR REPLACE TABLE `{ANALYTICS_TABLE}`
    PARTITION BY DATE(update_timestamp)
    CLUSTER BY issuer_id, status, store_state AS
    SELECT
      *,
      TIMESTAMP_DIFF(evaluation_end_ts, evaluation_start_ts, MILLISECOND) AS evaluation_duration_ms
    FROM `{STG_TABLE}`
    """
    sql_2 = f"""
    CREATE OR REPLACE TABLE `{ANALYTICS_DAILY_TABLE}` AS
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
    FROM `{STG_TABLE}`
    GROUP BY 1,2,3,4,5
    """
    sql_3 = f"""
    CREATE OR REPLACE TABLE `{ANALYTICS_KPIS_TABLE}` AS
    SELECT
      CURRENT_TIMESTAMP() AS generated_at,
      COUNT(*) AS total_evaluations,
      COUNTIF(status = 'APPROVED') AS total_approved,
      COUNTIF(status = 'DENIED') AS total_denied,
      SAFE_DIVIDE(COUNTIF(status = 'APPROVED'), COUNT(*)) AS approval_rate,
      AVG(bvs_score) AS avg_bvs_score,
      AVG(conventional_score) AS avg_conventional_score,
      AVG(TIMESTAMP_DIFF(evaluation_end_ts, evaluation_start_ts, MILLISECOND)) AS avg_duration_ms
    FROM `{STG_TABLE}`
    """
    bq_client.query(sql_1).result()
    bq_client.query(sql_2).result()
    bq_client.query(sql_3).result()


def refresh_ml():
    sql_1 = f"""
    CREATE OR REPLACE TABLE `{ML_FEATURES_TABLE}`
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
    FROM `{STG_TABLE}`
    """
    sql_2 = f"""
    CREATE OR REPLACE TABLE `{ML_TRAINING_TABLE}` AS
    SELECT *
    FROM `{ML_FEATURES_TABLE}`
    WHERE target_approved IS NOT NULL
    """
    sql_3 = f"""
    CREATE OR REPLACE TABLE `{ML_ONLINE_TABLE}`
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
    FROM `{ML_FEATURES_TABLE}`
    """
    bq_client.query(sql_1).result()
    bq_client.query(sql_2).result()
    bq_client.query(sql_3).result()


@app.route("/", methods=["POST"])
def receive_pubsub_push():
    envelope = request.get_json(silent=True)

    if not envelope or "message" not in envelope:
        return jsonify({"error": "invalid pubsub push body"}), 400

    pubsub_message = envelope["message"]
    data_b64 = pubsub_message.get("data", "")

    if not data_b64:
        return jsonify({"error": "missing message.data"}), 400

    message_data = base64.b64decode(data_b64).decode("utf-8")

    try:
        payload_json = json.loads(message_data)
    except Exception as e:
        write_quarantine(None, None, None, None, message_data, "json_parse", str(e))
        write_audit(None, None, None, "ERROR", "json_parse", str(e))
        return jsonify({"status": "quarantined", "reason": "invalid json"}), 200

    event_id = payload_json.get("event_id")
    entity_id = payload_json.get("entity_id")
    event_type = payload_json.get("event_type")
    event_timestamp = payload_json.get("event_timestamp")

    is_valid, validation_message = validate_payload(payload_json)
    if not is_valid:
        write_quarantine(
            event_id,
            entity_id,
            event_type,
            event_timestamp,
            message_data,
            "validation",
            validation_message
        )
        write_audit(event_id, entity_id, event_type, "ERROR", "validation", validation_message)
        return jsonify({"status": "quarantined", "reason": validation_message}), 200

    try:
        row = {
            "event_id": str(event_id),
            "entity_id": entity_id,
            "event_type": event_type,
            "event_timestamp": event_timestamp,
            "payload": message_data,
            "published_at": iso_now()
        }

        raw_row_id = build_row_id(event_id, entity_id, event_type)

        errors = bq_client.insert_rows_json(
            RAW_TABLE,
            [row],
            row_ids=[raw_row_id]
        )
        if errors:
            raise Exception(str(errors))

        write_audit(event_id, entity_id, event_type, "SUCCESS", "raw_insert", "evento gravado na raw")

        run_merge_staging()
        write_audit(event_id, entity_id, event_type, "SUCCESS", "staging_merge", "merge executado com sucesso")

        refresh_analytics()
        write_audit(event_id, entity_id, event_type, "SUCCESS", "analytics_refresh", "camada analítica atualizada")

        refresh_ml()
        write_audit(event_id, entity_id, event_type, "SUCCESS", "ml_refresh", "camada ml atualizada")

        return jsonify({"status": "processed"}), 200

    except Exception as e:
        write_quarantine(
            event_id,
            entity_id,
            event_type,
            event_timestamp,
            message_data,
            "processing",
            str(e)
        )
        write_audit(event_id, entity_id, event_type, "ERROR", "processing", str(e))
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)