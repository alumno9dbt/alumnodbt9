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
        LOWER(TRIM(t.value::string)) AS type
    FROM base,
         LATERAL FLATTEN(input => SPLIT(REPLACE(type_line, '—', ''), ' ')) t
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
