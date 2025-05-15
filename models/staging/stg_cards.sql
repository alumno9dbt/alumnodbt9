{{ config(materialized='view') }}

-- CTE que toma el JSON completo (solo un set por archivo)
WITH set_data AS (
    SELECT 
        -- El campo raw contiene el JSON completo del set
        raw AS set_object
    FROM {{ source('bronze', 'raw_printings') }}
),

-- Explota el array "cards" en filas → cada fila será una carta
cards_exploded AS (
    SELECT
        -- Saca el código del set
        set_object:"code"::string AS set_code,

        -- c.value contiene el JSON completo de la carta
        c.value AS card_object
    FROM set_data,
         -- Explota el array de cartas
         LATERAL FLATTEN(input => set_object:"cards") c
)

-- Selecciona y tipifica los campos de cada carta
SELECT
    -- Código del set
    set_code,

    -- Clave principal: mtgjsonV4Id o scryfallId (si falta mtgjson)
    COALESCE(
        card_object:identifiers:mtgjsonV4Id::string,
        card_object:identifiers:scryfallId::string
    ) AS card_id,

    -- Datos básicos de la carta
    card_object:name::string AS card_name,
    card_object:type::string AS type_line,
    card_object:manaCost::string AS mana_cost,
    card_object:rarity::string AS rarity,
    card_object:setCode::string AS set_code_card,

    -- Colores como array (para explotar en stg_card_colors) y como string (para BI)
    card_object:colors AS colors_array,
    ARRAY_TO_STRING(card_object:colors, ', ') AS colors_string,

    -- Atributos específicos (solo algunas cartas los tienen)
    card_object:power::string AS power,
    card_object:toughness::string AS toughness,
    card_object:loyalty::string AS loyalty,
    card_object:flavorText::string AS flavor_text,

    -- Identificadores externos para posibles joins
    card_object:identifiers:scryfallId::string AS scryfall_id,
    card_object:identifiers:mtgjsonV4Id::string AS mtgjsonv4_id

FROM cards_exploded

-- Filtro para evitar filas sin clave primaria
WHERE card_id IS NOT NULL
