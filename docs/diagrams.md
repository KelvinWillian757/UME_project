# Diagrama lógico

```text
Supabase/Postgres
  -> Trigger SQL
  -> Cloud Run Publisher
  -> Pub/Sub Topic
  -> Push Subscription
  -> Cloud Run Consumer
  -> BigQuery Raw
  -> BigQuery Staging
  -> Analytics
  -> ML
```
