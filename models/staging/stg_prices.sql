{{ config(materialized='view') }}

WITH raw_prices AS (
    SELECT
        card.key::string AS mtgjsonv4_id,
        card.value AS price_data
    FROM {{ source('bronze', 'raw_prices') }},
         LATERAL FLATTEN(input => raw) AS card
),

-- Vincula el ID de precios (mtgjsonv4_id) con el ID de cartas (scryfallId)
joined AS (
    SELECT
        i.card_id,
        rp.price_data:"paper" AS paper_prices
    FROM raw_prices rp
    JOIN {{ ref('stg_identifiers') }} i
      ON rp.mtgjsonv4_id = i.mtgjsonv4_id
    WHERE rp.price_data:"paper" IS NOT NULL
),

flattened AS (
    SELECT
        card_id,
        provider.key::string AS provider,
        provider.value AS provider_data
    FROM joined,
         LATERAL FLATTEN(input => paper_prices) AS provider
    WHERE provider.value:retail:normal IS NOT NULL
)

SELECT
    card_id,
    provider,
    TRY_PARSE_JSON(provider_data:retail:normal) AS retail_prices
FROM flattened
