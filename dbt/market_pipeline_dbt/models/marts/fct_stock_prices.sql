{{
    config(
        materialized='incremental',
        unique_key=['ticker', 'price_date']
    )
}}

SELECT
    ticker,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    fetched_at,

    -- Variation par rapport à la veille (window function)
    close_price - LAG(close_price) OVER (
        PARTITION BY ticker ORDER BY price_date
    ) AS variation_absolue,

    ROUND(
        (
            (close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY price_date))
            / NULLIF(LAG(close_price) OVER (PARTITION BY ticker ORDER BY price_date), 0) * 100
        )::numeric,
        2
    ) AS variation_pct,

    -- Moyenne mobile sur 5 jours (window function)
    ROUND(
        (AVG(close_price) OVER (
            PARTITION BY ticker ORDER BY price_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ))::numeric,
        2
    ) AS moyenne_mobile_5j

FROM {{ ref('stg_stock_prices') }}

{% if is_incremental() %}
WHERE price_date > (SELECT MAX(price_date) FROM {{ this }})
{% endif %}
