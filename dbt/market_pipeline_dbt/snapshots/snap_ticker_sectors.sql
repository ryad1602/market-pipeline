{% snapshot snap_ticker_sectors %}

{{
    config(
        target_schema='public',
        unique_key='ticker',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

SELECT * FROM {{ source('raw', 'ticker_sectors') }}

{% endsnapshot %}
