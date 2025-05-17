{{ config(materialized='view') }}

SELECT
    card_id,
    card_id AS scryfall_id
FROM {{ ref('stg_cards') }}
WHERE card_id IS NOT NULL
