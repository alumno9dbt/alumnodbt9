{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        card_id,
        card_name,
        type_line,
        mana_cost,
        set_code,
        set_code_card,
        power,
        toughness,
        flavor_text
    FROM {{ ref('stg_cards') }}
    WHERE card_id IS NOT NULL
),

colors AS (
    SELECT
        card_id,
        LISTAGG(color, ', ') AS color_names
    FROM {{ ref('stg_card_colors') }}
    GROUP BY card_id
),

types AS (
    SELECT
        card_id,
        LISTAGG(type, ', ') AS type_names
    FROM {{ ref('stg_card_types') }}
    GROUP BY card_id
),

rarities AS (
    SELECT *
    FROM {{ ref('stg_card_rarities') }}
)

SELECT
    b.card_id,
    b.card_name,
    b.type_line,
    t.type_names,
    c.color_names,
    b.mana_cost,
    b.power,
    b.toughness,
    r.rarity,
    b.flavor_text,
    b.set_code,
    b.set_code_card
FROM base b
LEFT JOIN colors c ON b.card_id = c.card_id
LEFT JOIN types t ON b.card_id = t.card_id
LEFT JOIN rarities r ON b.card_id = r.card_id
