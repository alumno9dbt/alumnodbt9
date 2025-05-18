{{ config(materialized='table') }}

WITH ids AS (
    -- IDs reales con precios en fact_card_prices
    SELECT DISTINCT card_id
    FROM {{ ref('fact_card_prices') }}
),

dim AS (
    SELECT
        ids.card_id,
        c.card_name,
        c.set_code_card AS set_code,
        c.rarity,
        ARRAY_TO_STRING(c.colors_array, ', ') AS colors,
        c.type_line
    FROM ids
    LEFT JOIN {{ ref('stg_cards') }} c
      ON ids.card_id = c.card_id
)

SELECT *
FROM dim
WHERE card_id IS NOT NULL
