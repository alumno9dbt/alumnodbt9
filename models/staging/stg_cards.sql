{{ config(materialized='view') }}

WITH set_data AS (
    SELECT raw AS set_object
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
    -- Clave primaria obligatoria y coherente
    card_object:identifiers:mtgjsonV4Id::string AS card_id,

    -- Información textual
    card_object:name::string AS card_name,
    card_object:type::string AS type_line,
    card_object:manaCost::string AS mana_cost,
    card_object:rarity::string AS rarity,

    -- Identificador del set (dos versiones)
    set_code,
    card_object:setCode::string AS set_code_card,

    -- Colores
    card_object:colors AS colors_array,
    ARRAY_TO_STRING(card_object:colors, ', ') AS colors_string,

    -- Atributos numéricos (si aplica)
    TRY_TO_NUMBER(card_object:power::string) AS power,
    TRY_TO_NUMBER(card_object:toughness::string) AS toughness,

    -- Otros detalles
    card_object:flavorText::string AS flavor_text,

    -- Identificadores externos
    card_object:identifiers:scryfallId::string AS scryfall_id,
    card_object:identifiers:mtgjsonV4Id::string AS mtgjsonv4_id

FROM cards_exploded
WHERE card_object:identifiers:mtgjsonV4Id IS NOT NULL
