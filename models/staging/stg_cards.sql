{{ config(materialized='view') }}

WITH printings AS (
    SELECT raw AS set_object
    FROM {{ source('bronze', 'raw_printings') }}
),

cards_exploded AS (
    SELECT
        set_object:"code"::string AS set_code,
        card.value AS card_object
    FROM printings,
         LATERAL FLATTEN(input => set_object:"cards") AS card
)

SELECT
    card_object:identifiers:scryfallId::string AS card_id,
    card_object:identifiers:mtgjsonV4Id::string AS mtgjsonv4_id,
    card_object:name::string AS card_name,
    card_object:type::string AS type_line,
    card_object:manaCost::string AS mana_cost,
    card_object:rarity::string AS rarity,
    set_code,
    card_object:setCode::string AS set_code_card,
    card_object:colors AS colors_array,
    ARRAY_TO_STRING(card_object:colors, ', ') AS colors_string,
    TRY_TO_NUMBER(card_object:power::string) AS power,
    TRY_TO_NUMBER(card_object:toughness::string) AS toughness,
    card_object:flavorText::string AS flavor_text
FROM cards_exploded
WHERE card_object:identifiers:scryfallId IS NOT NULL
  AND card_object:identifiers:mtgjsonV4Id IS NOT NULL
