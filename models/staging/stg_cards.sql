{{ config(materialized='view') }}

WITH cards_exploded AS (
    -- Explota el array de cartas directamente desde raw
    SELECT
        c.value AS card_object
    FROM {{ source('bronze', 'raw_printings') }},
         LATERAL FLATTEN(input => raw:"cards") c
)

SELECT
    -- Código del set al que pertenece la carta
    card_object:setCode::string AS set_code,

    -- Clave única de la carta
    COALESCE(
        card_object:identifiers:mtgjsonV4Id::string,
        card_object:identifiers:scryfallId::string
    ) AS card_id,

    -- Datos principales
    card_object:name::string AS card_name,
    card_object:type::string AS type_line,
    card_object:manaCost::string AS mana_cost,
    card_object:rarity::string AS rarity,

    -- Colores en array y string
    card_object:colors AS colors_array,
    ARRAY_TO_STRING(card_object:colors, ', ') AS colors_string,

    -- Atributos adicionales
    card_object:power::string AS power,
    card_object:toughness::string AS toughness,
    card_object:loyalty::string AS loyalty,
    card_object:flavorText::string AS flavor_text,

    -- Identificadores externos
    card_object:identifiers:scryfallId::string AS scryfall_id,
    card_object:identifiers:mtgjsonV4Id::string AS mtgjsonv4_id

FROM cards_exploded
