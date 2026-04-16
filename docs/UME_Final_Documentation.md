# 📄 UME Credit Case – Documentação Final

## 👤 Autor
Kelvin Willian  

---

# 🧩 Task 1 — Arquitetura da Solução

## 🎯 Objetivo

Construir um pipeline de dados escalável, resiliente e orientado a eventos para ingestão, processamento e disponibilização de dados de crédito em tempo real.

---

## 🚨 Problemas identificados

1. Forte acoplamento entre aplicações e ingestão  
2. Risco de duplicidade e perda de eventos  
3. Ausência de carga histórica confiável  
4. Necessidade de processamento em tempo real  

---

## 🏗️ Arquitetura Proposta

```
Supabase (Postgres)
   ↓
Webhook / Trigger
   ↓
Cloud Run (Publisher)
   ↓
Pub/Sub (mensageria)
   ↓
Cloud Run (Consumer)
   ↓
BigQuery (Raw → Staging → Analytics → ML)
```

---

## 🔧 Componentes

### 1. Supabase
- Origem dos dados
- Trigger para emissão de eventos

### 2. Cloud Run - Publisher
- Recebe eventos via webhook
- Publica no Pub/Sub

### 3. Pub/Sub
- Desacoplamento entre produtor e consumidor
- Garantia de entrega
- Escalabilidade

### 4. Cloud Run - Consumer
Responsável por:
- Persistência raw
- Merge na staging
- Atualização de analytics
- Atualização de ML
- Tratamento de erro (quarentena + auditoria)

### 5. BigQuery

Camadas:

#### Raw
- Dados brutos (imutáveis)

#### Staging
- Dados estruturados
- Deduplicação
- Idempotência

#### Analytics
- Visões para BI

#### ML
- Features
- Dados de treino
- Dados para inferência

---

## 📌 Decisões Arquiteturais

### Event-driven
Permite:
- processamento em tempo real
- desacoplamento
- escalabilidade horizontal

### Idempotência (MERGE)
Evita:
- duplicidade
- inconsistência

### Quarentena
Garante:
- pipeline resiliente
- não quebra em erro de dado

### Auditoria
Permite:
- rastreabilidade completa
- debugging

---

## 💰 Otimização de custo

- Cloud Run escala sob demanda
- Pub/Sub cobra por uso
- BigQuery com particionamento e clusterização

---

## 📈 Escalabilidade

- Pub/Sub suporta alto throughput
- Cloud Run escala automaticamente
- BigQuery é serverless

---

# 🧩 Task 2 — Implementação

## 🔁 Fluxo de Dados

1. Evento criado no Supabase  
2. Trigger envia para webhook  
3. Publisher envia para Pub/Sub  
4. Consumer processa  
5. Dados persistidos no BigQuery  

---

## 🧱 Camadas implementadas

### Raw
Tabela:
- raw_credit_evaluations_events

### Staging
Tabela:
- stg_credit_evaluations

Regras:
- deduplicação por evaluation_id
- latest update wins

---

### Analytics
Tabelas:
- analytics_credit_evaluations
- analytics_credit_evaluations_daily
- analytics_credit_kpis

Uso:
- dashboards
- KPIs

---

### ML
Tabelas:
- ml_features_credit_evaluations
- ml_training_credit_evaluations
- ml_online_credit_features

Uso:
- treino de modelos
- inferência

---

## 🛡️ Tratamento de erros

### Quarentena
Tabela:
- quarantine_credit_evaluations_events

Tipos de erro:
- JSON inválido
- campos obrigatórios ausentes
- erro de processamento

### Auditoria
Tabela:
- audit_credit_evaluations_pipeline

Rastreia:
- status
- etapa
- mensagens

---

## 🔁 Idempotência

Implementada via:
- MERGE no BigQuery
- chave: evaluation_id
- ordenação por update_timestamp

---

## ⚙️ Automação

Pipeline 100% automático:

- sem scheduler
- acionado por evento
- processamento imediato

---

## 🧪 Testes realizados

### Evento válido
✔ inserido com sucesso  
✔ propagado por todas as camadas  

### Evento inválido
✔ direcionado para quarentena  
✔ registrado na auditoria  
✔ não reprocessado  

---

## 🚀 Resultados alcançados

✔ ingestão desacoplada  
✔ processamento em tempo real  
✔ tolerância a falhas  
✔ rastreabilidade completa  
✔ base pronta para BI e ML  

---

## 🔮 Evoluções futuras

- Dataflow para micro-batch
- Feature Store
- Monitoramento com Cloud Monitoring
- Orquestração com Airflow

---

## 📌 Conclusão

A solução atende todos os requisitos do desafio, entregando um pipeline moderno, escalável e resiliente, alinhado às melhores práticas de engenharia de dados.
