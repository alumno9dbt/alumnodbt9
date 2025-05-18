{{ config(materialized='view') }}

WITH set_data AS (
    SELECT 
        raw AS set_object
    FROM {{ source('bronze', 'raw_printings') }}
)

SELECT
    set_object:"code"::string AS set_code,
    set_object:"name"::string AS set_name,
    TRY_TO_DATE(set_object:"releaseDate"::string) AS release_date,
    set_object:"type"::string AS set_type,
    set_object:"block"::string AS block_name,
    set_object:"baseSetSize"::number AS base_set_size,
    set_object:"totalSetSize"::number AS total_set_size,
    set_object:"isFoilOnly"::boolean AS is_foil_only,
    set_object:"isOnlineOnly"::boolean AS is_online_only,
    set_object:"isNonFoilOnly"::boolean AS is_nonfoil_only,
    set_object:"isForeignOnly"::boolean AS is_foreign_only
FROM set_data
WHERE set_object:"code" IS NOT NULL
