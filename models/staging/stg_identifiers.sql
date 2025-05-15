{{ config(materialized='view') }}

SELECT
    card_id,
    scryfall_id,
    mtgjsonv4_id
FROM {{ ref('stg_cards') }}
WHERE card_id IS NOT NULL
