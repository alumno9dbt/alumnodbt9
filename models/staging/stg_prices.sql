{{ config(materialized='view') }}

WITH exploded AS (
    SELECT
        k.key::string AS card_id,
        k.value:"paper" AS paper_prices,
        k.value:"mtgo" AS mtgo_prices
    FROM {{ source('bronze', 'raw_prices') }},
         LATERAL FLATTEN(input => raw) AS k
)

SELECT *
FROM exploded
WHERE card_id IS NOT NULL
