{{ config(materialized='view') }}

WITH prices_raw AS (
    SELECT
        raw:"uuid"::string AS card_id,
        raw:"paper" AS paper_prices,
        raw:"mtgo" AS mtgo_prices
    FROM {{ source('bronze', 'raw_prices') }}
    WHERE raw:"uuid" IS NOT NULL
)

SELECT
    card_id,
    paper_prices,
    mtgo_prices
FROM prices_raw

