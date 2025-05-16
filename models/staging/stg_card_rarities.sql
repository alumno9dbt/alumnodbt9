{{ config(materialized='view') }}

SELECT
    card_id,
    rarity
FROM {{ ref('stg_cards') }}
WHERE rarity IS NOT NULL
