{{ config(materialized='view') }}

WITH cards_flattened AS (
    SELECT
        card.key::string AS raw_card_id,
        card.value AS price_block
    FROM {{ source('bronze', 'raw_prices') }},
         LATERAL FLATTEN(input => raw) AS card
),

providers AS (
    SELECT
        raw_card_id,
        provider.key::string AS provider_name,
        TRY_PARSE_JSON(provider.value:retail:normal) AS retail_normal_prices
    FROM cards_flattened,
         LATERAL FLATTEN(input => price_block:paper) AS provider
    WHERE provider.value:retail:normal IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['raw_card_id', 'provider_name']) }} AS card_id,
    raw_card_id,
    provider_name AS provider,
    retail_normal_prices
FROM providers
