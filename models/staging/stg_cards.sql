{{ config(materialized='view') }}

WITH set_data AS (
    -- Cada archivo contiene solo un set → sacamos directamente el objeto del set
    SELECT 
        raw AS set_object
    FROM {{ source('bronze', 'raw_printings') }}
),

cards_exploded AS (
    SELECT
        set_object:"code"::string AS set_code,
        c.value AS card_object
    FROM set_data,
         LATERAL FLATTEN(input => set_object:"cards") c
)

SELECT
    set_code,

    COALESCE(
        card_object:identifiers:mtgjsonV4Id::string,
        card_object:identifiers:scryfallId::string
    ) AS card_id,

    card_object:name::string AS card_name,
    card_object:type::string AS type_line,
    card_object:manaCost::string AS mana_cost,
    card_object:rarity::string AS rarity,
    card_object:setCode::string AS set_code_card,

    card_object:colors AS colors_array,
    ARRAY_TO_STRING(card_object:colors, ', ') AS colors_string,

    card_object:power::string AS power,
    card_object:toughness::string AS toughness,
    card_object:loyalty::string AS loyalty,
    card_object:flavorText::string AS flavor_text,

    card_object:identifiers:scryfallId::string AS scryfall_id,
    card_object:identifiers:mtgjsonV4Id::string AS mtgjsonv4_id
FROM cards_exploded
WHERE card_id IS NOT NULL
