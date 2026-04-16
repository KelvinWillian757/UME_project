# 📘 UME Credit Case – Engenharia de Dados

## 🚀 Visão Geral

Este projeto implementa um pipeline de dados orientado a eventos para ingestão, processamento, análise e geração de features de Machine Learning para avaliações de crédito.

Tecnologias utilizadas:
- Google Pub/Sub
- Cloud Run
- BigQuery
- Supabase (PostgreSQL)

---

## 🧠 Problema

- Forte acoplamento entre aplicações e ingestão de dados  
- Risco de duplicidade ou perda de eventos  
- Ausência de mecanismo eficiente para carga histórica  
- Necessidade de dados em tempo real  

---

## ✅ Solução

- Arquitetura event-driven
- Desacoplamento via Pub/Sub
- Processamento idempotente com MERGE
- Quarentena para eventos inválidos
- Auditoria completa do pipeline
- Camadas analítica e de Machine Learning

---

## 🏗️ Arquitetura

Supabase (Postgres)
   ↓ (trigger / webhook)
Cloud Run - Publisher  
https://supabase-webhook-publisher-tvkp4tulsq-uc.a.run.app
   ↓
Pub/Sub Topic  
credit-evaluations-topic
   ↓
Cloud Run - Consumer  
https://credit-events-consumer-tvkp4tulsq-uc.a.run.app
   ↓
BigQuery (us-central1)

---

## 📂 Estrutura do Projeto

ume-credit-case/
  supabase/
  cloud_run_publisher/
  cloud_run_consumer/
  bigquery/
    ddl/
    transformations/
    analytics/
    ml/
    validation/
  docs/

---

## ⚙️ Ambiente

- Projeto: ume
- Região: us-central1
- Service Account: powerbi-odbc@ume.iam.gserviceaccount.com

### Datasets BigQuery

- ume.staging → pipeline, analytics e ML  
- ume.business_analytics_gold → existente  
- ume.business_analytics_silver → existente  

---

## 🚀 Deploy

### Publisher

cd cloud_run_publisher

gcloud run deploy supabase-webhook-publisher \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --service-account=powerbi-odbc@ume.iam.gserviceaccount.com

---

### Consumer

cd cloud_run_consumer

gcloud run deploy credit-events-consumer \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --service-account=powerbi-odbc@ume.iam.gserviceaccount.com

---

### Pub/Sub

gcloud pubsub topics create credit-evaluations-topic

gcloud pubsub subscriptions create credit-evaluations-push-sub \
  --topic=credit-evaluations-topic \
  --push-endpoint=https://credit-events-consumer-tvkp4tulsq-uc.a.run.app

---

## 🧪 Testes

### Evento válido

SELECT * FROM `ume.staging.raw_credit_evaluations_events`;
SELECT * FROM `ume.staging.stg_credit_evaluations`;
SELECT * FROM `ume.staging.analytics_credit_evaluations`;
SELECT * FROM `ume.staging.ml_features_credit_evaluations`;

---

### Evento inválido

SELECT * FROM `ume.staging.quarantine_credit_evaluations_events`;
SELECT * FROM `ume.staging.audit_credit_evaluations_pipeline`;

---

## 🔁 Fluxo de Processamento

1. Evento criado no Supabase  
2. Webhook envia para o Publisher  
3. Publisher publica no Pub/Sub  
4. Pub/Sub envia para o Consumer  
5. Consumer:
   - grava na camada raw  
   - realiza merge na staging  
   - atualiza analytics  
   - atualiza ML  
   - trata erros (quarentena + auditoria)  

---

## ⚠️ Observações

- Pipeline 100% orientado a eventos (sem scheduler)  
- Processamento idempotente via MERGE  
- Quarentena evita quebra do pipeline  
- Auditoria garante rastreabilidade  

---

## 📈 Evoluções Futuras

- Uso de Dataflow para processamento em lote  
- Transformações incrementais  
- Integração com Feature Store  
- Monitoramento com Cloud Monitoring  

---

## 🎯 Resultado

✔ ingestão desacoplada  
✔ tolerância a falhas  
✔ processamento em tempo real  
✔ rastreabilidade completa  
✔ base pronta para analytics e ML  
