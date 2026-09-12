SELECT
    "Date" AS price_date,
    ticker,
    "Open" AS open_price,
    "High" AS high_price,
    "Low" AS low_price,
    "Close" AS close_price,
    "Volume" AS volume,
    fetched_at
FROM {{ source('raw', 'raw_stock_prices') }}
WHERE "Close" IS NOT NULL
