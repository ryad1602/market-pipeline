SELECT
    coin,
    price_usd,
    market_cap_usd,
    volume_24h_usd,
    change_24h_pct,
    fetched_at
FROM {{ source('raw', 'raw_crypto_prices') }}
