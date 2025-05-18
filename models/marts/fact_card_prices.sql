{{ config(
    materialized = 'incremental',
    unique_key = ['card_id', 'provider', 'channel', 'product_type', 'price_date']
) }}

WITH base AS (
    SELECT
        card_id,
        provider.key::string AS provider,
        provider.value AS provider_data
    FROM {{ ref('stg_prices') }},
         LATERAL FLATTEN(input => paper_prices) AS provider
),

channels AS (
    SELECT
        card_id,
        provider,
        channel.key::string AS channel,
        channel.value AS channel_data
    FROM base,
         LATERAL FLATTEN(input => provider_data) AS channel
),

types AS (
    SELECT
        card_id,
        provider,
        channel,
        type.key::string AS product_type,
        type.value AS prices_by_date
    FROM channels,
         LATERAL FLATTEN(input => channel_data) AS type
),

final_prices AS (
    SELECT
        card_id,
        provider,
        channel,
        product_type,
        TRY_TO_DATE(date_price.key::string) AS price_date,
        date_price.value::float AS price
    FROM types,
         LATERAL FLATTEN(input => prices_by_date) AS date_price
)

SELECT
    card_id,
    provider,
    channel,
    product_type,
    price_date,
    price AS mid_price
FROM final_prices
WHERE card_id IS NOT NULL
  AND price_date IS NOT NULL
  AND price IS NOT NULL
  {% if is_incremental() %}
    AND price_date > COALESCE((SELECT MAX(price_date) FROM {{ this }}), '1900-01-01')
  {% endif %}
