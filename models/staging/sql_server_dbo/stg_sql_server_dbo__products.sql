
{{
  config(
    materialized='view'
  )
}}

WITH src_products AS (

    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}

),

products_transform AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_id,
        -- Vamos a pasar price a order_items
        name,
        inventory,
        CASE
            WHEN _fivetran_deleted IS NULL THEN FALSE
            ELSE _fivetran_deleted
        END AS _fivetran_deleted,
        CONVERT_TIMEZONE('Europe/Madrid', 'UTC', CAST(_fivetran_synced AS TIMESTAMP_NTZ)) AS _fivetran_synced

    FROM src_products

)

select * from products_transform
