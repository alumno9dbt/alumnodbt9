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
