WITH src_address AS (

    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}

),

address_transform AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_id,
        country,
        state,
        LPAD(CAST(zipcode AS STRING), 5, '0') AS zipcode,
        address,
        CASE
            WHEN _fivetran_deleted IS NULL THEN FALSE
            ELSE _fivetran_deleted
        END AS _fivetran_deleted,
        CONVERT_TIMEZONE('Europe/Madrid', 'UTC', CAST(_fivetran_synced AS TIMESTAMP_NTZ)) AS _fivetran_synced

    FROM src_address

)

SELECT * FROM address_transform
