WITH src_orders AS (

    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}

),

orders_transform AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_id,
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_id,
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_id,
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_id,
        created_at,
        delivered_at,
        estimated_delivery_at,
        order_cost,
        order_total,
        status,
        shipping_service,
        shipping_cost,
        CASE
            WHEN _fivetran_deleted IS NULL THEN FALSE
            ELSE _fivetran_deleted
        END AS _fivetran_deleted,
        CONVERT_TIMEZONE('Europe/Madrid', 'UTC', CAST(_fivetran_synced AS TIMESTAMP_NTZ)) AS _fivetran_synced

    FROM src_orders

)

SELECT * FROM orders_transform

