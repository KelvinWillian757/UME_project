#ML

SELECT * FROM `assertiv.staging.ml_training_credit_evaluations` LIMIT 1000

SELECT * FROM `assertiv.staging.ml_online_credit_features` LIMIT 1000

SELECT * FROM `assertiv.staging.ml_features_credit_evaluations` LIMIT 1000

#Gold

SELECT * FROM `assertiv.staging.analytics_credit_evaluations` LIMIT 1000

SELECT * FROM `assertiv.staging.analytics_credit_evaluations_daily` LIMIT 1000

SELECT * FROM `assertiv.staging.analytics_credit_kpis` LIMIT 1000

#Raw

SELECT * FROM `assertiv.staging.raw_credit_evaluations_events` LIMIT 1000

SELECT * FROM `assertiv.staging.stg_credit_evaluations` LIMIT 1000

#audit, quality

SELECT * FROM `assertiv.staging.audit_credit_evaluations_pipeline` LIMIT 1000

SELECT * FROM `assertiv.staging.quarantine_credit_evaluations_events` LIMIT 1000
