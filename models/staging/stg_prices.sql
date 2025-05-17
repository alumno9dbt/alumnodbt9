{{ config(materialized='view') }}

WITH raw_prices AS (
    SELECT
        card.key::string AS mtgjsonv4_id,
        card.value AS card_data
    FROM {{ source('bronze', 'raw_prices') }},
         LATERAL FLATTEN(input => raw) card
),

identifiers AS (
    SELECT
        card.value:identifiers:mtgjsonV4Id::string AS mtgjsonv4_id,
        card.value:identifiers:scryfallId::string AS card_id
    FROM {{ source('bronze', 'raw_printings') }},
         LATERAL FLATTEN(input => raw:"cards") card
    WHERE card.value:identifiers:mtgjsonV4Id IS NOT NULL
      AND card.value:identifiers:scryfallId IS NOT NULL
),

joined AS (
    SELECT
        i.card_id,
        rp.card_data:"paper" AS paper_prices
    FROM raw_prices rp
    JOIN identifiers i ON rp.mtgjsonv4_id = i.mtgjsonv4_id
    WHERE rp.card_data:"paper" IS NOT NULL
),

exploded AS (
    SELECT
        card_id,
        provider.key::string AS provider,
        provider.value AS provider_data
    FROM joined,
         LATERAL FLATTEN(input => paper_prices) provider
    WHERE provider.value:retail:normal IS NOT NULL
)

SELECT
    card_id,
    provider,
    TRY_PARSE_JSON(provider_data:retail:normal) AS retail_prices
FROM exploded
