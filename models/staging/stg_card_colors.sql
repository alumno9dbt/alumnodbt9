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
        color.value::string AS color
    FROM base,
         LATERAL FLATTEN(input => colors_array) AS color
)

SELECT *
FROM colors_exploded
