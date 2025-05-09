WITH src_users AS (

    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}

),

user_transform AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_id,
        first_name,
        last_name,
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_id,
        phone_number,
        email,
        CONVERT_TIMEZONE('Europe/Madrid', 'UTC', CAST(created_at AS TIMESTAMP_NTZ)) AS created_at,
        CONVERT_TIMEZONE('Europe/Madrid', 'UTC', CAST(updated_at AS TIMESTAMP_NTZ)) AS updated_at,
        CASE
            WHEN _fivetran_deleted IS NULL THEN FALSE
            ELSE _fivetran_deleted
        END AS _fivetran_deleted,
        CONVERT_TIMEZONE('Europe/Madrid', 'UTC', CAST(_fivetran_synced AS TIMESTAMP_NTZ)) AS _fivetran_synced

    FROM src_users

)

SELECT * FROM user_transform
