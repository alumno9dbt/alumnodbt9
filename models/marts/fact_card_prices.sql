{{ config(
    materialized = 'incremental',
    unique_key = ['card_id', 'price_date', 'provider'],
    on_schema_change = 'ignore',
    incremental_strategy = 'append'
) }}

WITH base AS (
    SELECT
        card_id,
        paper_prices
    FROM {{ ref('stg_prices') }}
    WHERE paper_prices IS NOT NULL
),

providers AS (
    SELECT
        card_id,
        provider.key::string AS provider_name,
        provider.value AS provider_data
    FROM base,
         LATERAL FLATTEN(input => paper_prices) provider
),

retail_normal AS (
    SELECT
        card_id,
        provider_name,
        TRY_PARSE_JSON(provider_data:retail:normal) AS retail_normal_prices
    FROM providers
    WHERE provider_data:retail:normal IS NOT NULL
),

prices_flattened AS (
    SELECT
        card_id,
        provider_name,
        price_entry.key::string AS price_date_str,
        price_entry.value::string AS price_value_str
    FROM retail_normal,
         LATERAL FLATTEN(input => retail_normal_prices) price_entry
)

SELECT
    card_id,
    provider_name AS provider,
    TO_DATE(price_date_str) AS price_date,
    TRY_TO_DECIMAL(price_value_str, 10, 2) AS price
FROM prices_flattened
WHERE TRY_TO_DECIMAL(price_value_str, 10, 2) IS NOT NULL
