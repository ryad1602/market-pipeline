WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker
            ORDER BY fetched_at DESC
        ) AS rn
    FROM {{ ref('stg_stock_prices') }}
)

SELECT
    ticker,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    fetched_at
FROM ranked
WHERE rn = 1
