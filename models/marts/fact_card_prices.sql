{{ config(materialized='table') }}

with prices as (
    select *
    from {{ ref('stg_prices') }}
),

cards as (
    select
        card_id,
        name,
        set_code,
        collector_number,
        rarity,
        rarity_rank,
        main_types,
        subtypes,
        color_names,
        cmc,
        layout,
        set_name,
        set_released_at,
        set_type
    from {{ ref('dim_cards') }}
)

select
    p.card_id,
    c.name,
    c.set_code,
    c.set_name,
    c.set_released_at,
    c.set_type,
    c.collector_number,
    c.rarity,
    c.rarity_rank,
    c.main_types,
    c.subtypes,
    c.color_names,
    c.cmc,
    c.layout,

    -- precios
    p.usd,
    p.usd_foil,
    p.eur,
    p.eur_foil,
    p.tix
from prices p
inner join cards c on p.card_id = c.card_id
