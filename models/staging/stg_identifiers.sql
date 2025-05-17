{{ config(materialized = 'view') }}

WITH printings AS (
    SELECT raw AS set_object
    FROM {{ source('bronze', 'raw_printings') }}
),

identifiers_flat AS (
    SELECT
        card.value:identifiers:scryfallId::string AS scryfall_id,
        card.value:identifiers:mtgjsonV4Id::string AS mtgjsonv4_id
    FROM printings,
         LATERAL FLATTEN(input => set_object:"cards") AS card
    WHERE card.value:identifiers:scryfallId IS NOT NULL
      AND card.value:identifiers:mtgjsonV4Id IS NOT NULL
)

SELECT DISTINCT
    mtgjsonv4_id,
    scryfall_id AS card_id
FROM identifiers_flat
WHERE mtgjsonv4_id IS NOT NULL
  AND card_id IS NOT NULL
