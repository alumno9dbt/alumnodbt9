{{ config(materialized='view') }}

WITH base AS (
    SELECT
        card_id,
        type_line,
        SPLIT(REPLACE(type_line, '—', ''), ' ') AS type_array
    FROM {{ ref('stg_cards') }}
    WHERE type_line IS NOT NULL
),

types_exploded AS (
    SELECT
        card_id,
        LOWER(TRIM(type.value::string)) AS type
    FROM base,
         LATERAL FLATTEN(input => type_array) AS type
),

cleaned AS (
    SELECT
        card_id,
        INITCAP(type) AS type
    FROM types_exploded
    WHERE type != ''
)

SELECT DISTINCT card_id, type
FROM cleaned
