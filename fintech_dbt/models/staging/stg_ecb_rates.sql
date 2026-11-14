SELECT
    CAST(rate_date AS DATE) AS rate_date,
    UPPER(TRIM(target_currency)) AS target_currency,
    CAST(ecb_rate AS DECIMAL(18, 6)) AS ecb_exchange_rate
FROM {{ source('raw_rates', 'ecb_rates') }}
