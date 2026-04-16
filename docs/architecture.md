# Arquitetura Técnica

## Objetivo
Construir um pipeline event-driven para avaliações de crédito com ingestão automática, rastreabilidade, deduplicação, consumo analítico e preparação para ML.

## Fluxo
1. Um evento é inserido em `public.credit_evaluations_events` no Supabase.
2. Um trigger SQL chama o Cloud Run publisher.
3. O publisher publica a mensagem no Pub/Sub.
4. A push subscription entrega a mensagem ao Cloud Run consumer.
5. O consumer grava a mensagem bruta na raw do BigQuery.
6. O consumer executa `MERGE` na staging.
7. O consumer atualiza as camadas analytics e ML.
8. Se houver erro de validação, a mensagem vai para quarentena e auditoria.

## Decisões de arquitetura
- **Outbox no Postgres** para desacoplar a emissão da aplicação.
- **Pub/Sub** para mensageria e desacoplamento.
- **Cloud Run** para serviços simples e gerenciados.
- **BigQuery** como armazenamento analítico e camada de features.
- **Quarentena** para eventos inválidos.
- **Auditoria** para observabilidade.

## Camadas
- **Raw**: evento bruto recebido do Pub/Sub.
- **Staging**: normalização, tipagem, deduplicação e `MERGE`.
- **Analytics**: consumo analítico.
- **ML**: features históricas, treino e inferência online.
