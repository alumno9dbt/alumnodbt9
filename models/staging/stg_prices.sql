{{ config(materialized='view') }}

WITH cards_flattened AS (
    SELECT
        card.value AS price_block,
        card.key AS card_id
    FROM {{ source('bronze', 'raw_prices') }},
         LATERAL FLATTEN(input => raw) AS card
)

SELECT
    card_id,
    price_block:"paper" AS paper_prices,
    price_block:"mtgo" AS mtgo_prices
FROM cards_flattened

