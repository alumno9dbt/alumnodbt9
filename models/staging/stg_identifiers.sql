{{ config(materialized='view') }}

SELECT
    card_id,
    scryfall_id,
    mtgjsonv4_id
FROM {{ ref('stg_cards') }}
WHERE card_id IS NOT NULL
  AND (scryfall_id IS NOT NULL OR mtgjsonv4_id IS NOT NULL)
