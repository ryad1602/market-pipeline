WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY coin
            ORDER BY fetched_at DESC
        ) AS rn
    FROM {{ ref('stg_crypto_prices') }}
)

SELECT
    coin,
    price_usd,
    market_cap_usd,
    volume_24h_usd,
    change_24h_pct,
    fetched_at
FROM ranked
WHERE rn = 1
