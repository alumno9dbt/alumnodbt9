{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        c.mtgjsonv4_id AS card_id, 
        c.card_name,
        c.type_line,
        c.mana_cost,
        c.set_code,
        c.set_code_card,
        c.power,
        c.toughness,
        c.flavor_text
    FROM {{ ref('stg_cards') }} c
    WHERE c.mtgjsonv4_id IS NOT NULL
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

identifiers AS (
    SELECT *
    FROM {{ ref('stg_identifiers') }}
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
    b.set_code_card,
    i.scryfall_id,
    i.mtgjsonv4_id
FROM base b
LEFT JOIN colors c     ON b.card_id = c.card_id
LEFT JOIN types t      ON b.card_id = t.card_id
LEFT JOIN identifiers i ON b.card_id = i.card_id
LEFT JOIN rarities r   ON b.card_id = r.card_id
