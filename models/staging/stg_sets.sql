{{ config(
    materialized='incremental',
    unique_key='set_code',
    incremental_strategy='merge',
    merge_update_columns = ['set_name', 'released_at', 'set_type']
) }}

with source as (
    select *
    from {{ source('bronze', 'sets') }}
),

cleaned as (
    select
        set_code,
        name as set_name,
        try_to_date(released_at) as released_at,
        set_type
    from source
    where set_code is not null
)

select *
from cleaned
{% if is_incremental() %}
    where try_to_date(released_at) >= (
        select max(released_at) from {{ this }}
    )
{% endif %}
