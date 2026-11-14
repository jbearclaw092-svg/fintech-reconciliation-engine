{{ config(materialized='table') }}

WITH txns AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

rates AS (
    SELECT * FROM {{ ref('stg_ecb_rates') }}
)

SELECT
    t.txn_id,
    t.transaction_date,
    t.target_currency,
    t.local_amount,
    t.recorded_eur_amount,
    r.ecb_exchange_rate,
    ROUND(t.local_amount / r.ecb_exchange_rate, 2) AS expected_eur_amount,
    t.recorded_eur_amount - ROUND(t.local_amount / r.ecb_exchange_rate, 2) AS variance_eur,
    CASE 
        WHEN ABS(t.recorded_eur_amount - (t.local_amount / r.ecb_exchange_rate)) 
             / NULLIF((t.local_amount / r.ecb_exchange_rate), 0) > 0.02 
        THEN 'DISCREPANCY_FLAGGED'
        ELSE 'RECONCILED'
    END AS reconciliation_status
FROM txns t
LEFT JOIN rates r
    ON t.transaction_date = r.rate_date
    AND t.target_currency = r.target_currency
