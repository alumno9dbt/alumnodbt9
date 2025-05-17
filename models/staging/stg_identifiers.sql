{{ config(materialized='view') }}

SELECT
    card_id,
    raw_card_id
FROM {{ ref('stg_cards') }}
WHERE card_id IS NOT NULL
