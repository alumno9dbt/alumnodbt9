{{ config(materialized='view') }}

SELECT
    -- Código del set (clave única para joins)
    raw:code::string AS set_code,

    -- Información principal del set
    raw:name::string AS set_name,
    raw:releaseDate::string AS release_date,
    raw:block::string AS block,
    raw:type::string AS set_type,

    -- Tamaños del set
    raw:baseSetSize::number AS base_set_size,
    raw:totalSetSize::number AS total_set_size,

    -- Otros campos interesantes
    raw:parentCode::string AS parent_code,
    raw:isFoilOnly::boolean AS is_foil_only,
    raw:isOnlineOnly::boolean AS is_online_only,
    raw:isPaperOnly::boolean AS is_paper_only

FROM {{ source('bronze', 'raw_printings') }}
