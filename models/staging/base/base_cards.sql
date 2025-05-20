{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns = ['oracle_id', 'name', 'set_code', 'collector_number', 'rarity', 'mana_cost', 'cmc', 'type_line', 'oracle_text', 'colors_array', 'color_identity_array', 'layout', 'released_at']
) }}

with source as (
    select *
    from {{ source('bronze', 'cards') }}
),

cleaned as (
    select
        id,
        oracle_id,
        name,
        "SET" as set_code,
        collector_number,
        rarity,
        mana_cost,
        cast(cmc as float) as cmc,
        type_line,
        oracle_text,
        split(colors, ',') as colors_array,
        split(color_identity, ',') as color_identity_array,
        layout,
        try_to_date(released_at) as released_at
    from source
    where id is not null
)

select *
from cleaned
{% if is_incremental() %}
    where try_to_date(released_at) >= (
        select max(released_at) from {{ this }}
    )
{% endif %}
