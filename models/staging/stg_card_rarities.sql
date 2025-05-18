{{ config(materialized='view') }}

SELECT
    card_id,
    rarity
FROM {{ ref('stg_cards') }}
WHERE card_id IS NOT NULL
  AND rarity IS NOT NULL
