{{ config(materialized='table') }}

with prices as (
    select *
    from {{ ref('fact_card_prices') }}
),

cards as (
    select
        card_id,
        name,
        set_name,
        rarity,
        main_types,
        color_names,
        case 
            when lower(main_types) like '%legendary%' then true
            else false
        end as is_legendary
    from {{ ref('dim_cards') }}
)

select
    p.card_id,
    c.name as card_name,
    c.set_name,
    c.rarity,
    c.main_types as type_name,
    c.color_names as color_name,
    c.is_legendary,
    p.usd,
    p.eur,
    p.tix
from prices p
left join cards c on p.card_id = c.card_id
