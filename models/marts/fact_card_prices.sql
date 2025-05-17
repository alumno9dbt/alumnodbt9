{{ config(
    materialized = 'incremental',
    unique_key = ['card_id', 'price_date', 'provider'],
    on_schema_change = 'ignore',
    incremental_strategy = 'append'
) }}

WITH retail_normal AS (
    SELECT
        card_id,
        provider,
        TRY_PARSE_JSON(retail_normal_prices) AS retail_normal_json
    FROM {{ ref('stg_prices') }}
    WHERE retail_normal_prices IS NOT NULL
),

prices_flattened AS (
    SELECT
        card_id,
        provider,
        price.key::string AS price_date_str,
        price.value::string AS price_value_str
    FROM retail_normal,
         LATERAL FLATTEN(input => retail_normal_json) price
)

SELECT
    card_id,
    provider,
    TO_DATE(price_date_str) AS price_date,
    TRY_TO_DECIMAL(price_value_str, 10, 2) AS price
FROM prices_flattened
WHERE TRY_TO_DECIMAL(price_value_str, 10, 2) IS NOT NULL

{% if is_incremental() %}
AND TO_DATE(price_date_str) > (
    SELECT COALESCE(MAX(price_date), '1900-01-01')
    FROM {{ this }}
)
{% endif %}
