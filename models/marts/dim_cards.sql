{{ config(materialized='table') }}

WITH base AS (
    SELECT
        card_id,
        card_name,
        type_line,
        mana_cost,
        rarity,
        power,
        toughness,
        flavor_text,
        set_code_card AS set_code
    FROM {{ ref('stg_cards') }}
),

colors AS (
    SELECT card_id, LISTAGG(color, ', ') AS colors
    FROM {{ ref('stg_card_colors') }}
    GROUP BY card_id
),

types AS (
    SELECT card_id, LISTAGG(type, ', ') AS types
    FROM {{ ref('stg_card_types') }}
    GROUP BY card_id
)

SELECT
    b.card_id,
    b.card_name,
    b.type_line,
    b.mana_cost,
    b.rarity,
    b.power,
    b.toughness,
    b.flavor_text,
    b.set_code,
    c.colors,
    t.types
FROM base b
LEFT JOIN colors c ON b.card_id = c.card_id
LEFT JOIN types t ON b.card_id = t.card_id
WHERE b.card_id IS NOT NULL
