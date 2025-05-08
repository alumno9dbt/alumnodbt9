
{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    UNION ALL
    SELECT 
        'no promo' AS promo_id,
        0 AS discount,
        'active' AS status,
        null AS _fivetran_deleted,
        sysdate() AS _fivetran_synced
    ),

promos_transform AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_id
        , promo_id AS promo_name
        , discount
        , status
        , _fivetran_deleted
        , _fivetran_synced AS date_load
    FROM src_promos
    )

SELECT * FROM promos_id_hash