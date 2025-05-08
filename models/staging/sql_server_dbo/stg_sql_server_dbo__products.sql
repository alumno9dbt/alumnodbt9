
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
        _fivetran_deleted,
        _fivetran_synced

    FROM src_products

)

select * from products_transform
