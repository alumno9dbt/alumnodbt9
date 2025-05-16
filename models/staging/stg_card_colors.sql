{{ config(materialized='view') }}

WITH base AS (
    SELECT
        card_id,
        colors_array
    FROM {{ ref('stg_cards') }}
    WHERE colors_array IS NOT NULL
),

colors_exploded AS (
    SELECT
        card_id,
        UPPER(TRIM(c.value::string)) AS color_code
    FROM base,
         LATERAL FLATTEN(input => colors_array) c
),

colors_named AS (
    SELECT
        card_id,
        CASE color_code
            WHEN 'W' THEN 'White'
            WHEN 'U' THEN 'Blue'
            WHEN 'B' THEN 'Black'
            WHEN 'R' THEN 'Red'
            WHEN 'G' THEN 'Green'
            ELSE color_code  -- por si aparece algo inesperado
        END AS color
    FROM colors_exploded
)

SELECT *
FROM colors_named
WHERE color IS NOT NULL
