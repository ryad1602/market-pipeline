SELECT
    price_date,
    ticker,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    fetched_at
FROM {{ source('raw', 'raw_stock_prices') }}
WHERE close_price IS NOT NULL
