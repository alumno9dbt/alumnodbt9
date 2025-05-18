{{ config(materialized='view') }}

WITH base AS (
    SELECT
        card_id,
        type_line
    FROM {{ ref('stg_cards') }}
    WHERE type_line IS NOT NULL
),

types_exploded AS (
    SELECT
        card_id,
        LOWER(TRIM(f.value::string)) AS raw_type
    FROM base,
         LATERAL FLATTEN(input => SPLIT(REPLACE(type_line, '—', ''), ' ')) AS f
),

cleaned AS (
    SELECT
        card_id,
        INITCAP(raw_type) AS type
    FROM types_exploded
    WHERE raw_type != ''
)

SELECT DISTINCT card_id, type
FROM cleaned
