with cards as (
    select
        id as card_id,
        colors_array
    from {{ ref('stg_cards') }}
),

exploded as (
    select
        card_id,
        trim(color.value::string) as color_initial
    from cards,
         lateral flatten(input => colors_array) as color
    where color.value is not null
),

labeled as (
    select
        card_id,
        color_initial,
        case color_initial
            when 'W' then 'White'
            when 'U' then 'Blue'
            when 'B' then 'Black'
            when 'R' then 'Red'
            when 'G' then 'Green'
            else 'Unknown'
        end as color_name
    from exploded
)

select *
from labeled
