{{ config(materialized='view') }}

WITH set_data AS (
    SELECT 
        raw AS set_object
    FROM {{ source('bronze', 'raw_printings') }}
)

SELECT
    -- Código del set
    set_object:"code"::string AS set_code,

    -- Nombre del set
    set_object:"name"::string AS set_name,

    -- Fecha de lanzamiento como DATE (antes era string)
    TRY_TO_DATE(set_object:"releaseDate"::string) AS release_date,

    -- Tipo de set (ej: expansion, core, promo)
    set_object:"type"::string AS set_type,

    -- Nombre del bloque (si aplica)
    set_object:"block"::string AS block_name,

    -- Tamaños
    set_object:"baseSetSize"::number AS base_set_size,
    set_object:"totalSetSize"::number AS total_set_size,

    -- Booleans
    set_object:"isFoilOnly"::boolean AS is_foil_only,
    set_object:"isOnlineOnly"::boolean AS is_online_only

FROM set_data
WHERE set_object:"code" IS NOT NULL
