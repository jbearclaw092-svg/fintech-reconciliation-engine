SELECT
    txn_id,
    CAST(txn_date AS DATE) AS transaction_date,
    UPPER(TRIM(currency)) AS target_currency,
    CAST(local_amount AS DECIMAL(18, 2)) AS local_amount,
    CAST(recorded_eur AS DECIMAL(18, 2)) AS recorded_eur_amount
FROM {{ source('raw_ledger', 'transactions') }}
