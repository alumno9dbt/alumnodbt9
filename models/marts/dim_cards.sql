{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('base_cards') }}
    where oracle_id is not null
),

colors as (
    select
        card_id,
        listagg(color_name, ', ') as color_names
    from {{ ref('stg_card_colors') }}
    group by card_id
),

types as (
    select
        card_id,
        main_types,
        subtypes
    from {{ ref('stg_card_types') }}
),

rarities as (
    select *
    from {{ ref('stg_card_rarities') }}
),

sets as (
    select
        set_code,
        set_name,
        released_at as set_released_at,
        set_type
    from {{ ref('stg_sets') }}
)

select
    b.id as card_id,
    b.oracle_id,
    b.name,
    b.set_code,
    s.set_name,
    s.set_released_at,
    s.set_type,
    b.collector_number,
    b.rarity,
    r.rarity_rank,
    b.mana_cost,
    b.cmc,
    t.main_types,
    t.subtypes,
    coalesce(c.color_names, 'Colorless') as color_names,
    b.layout,
    b.oracle_text,
    b.released_at
from base b
left join types t on b.id = t.card_id
left join colors c on b.id = c.card_id
left join rarities r on lower(b.rarity) = r.rarity
left join sets s on b.set_code = s.set_code
