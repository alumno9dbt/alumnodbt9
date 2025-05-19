{{ config(
    materialized='incremental',
    unique_key='card_id || dbt_valid_from',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "dbt_valid_from", "data_type": "date"}
) }}

with snapshot_data as (
    select *
    from {{ ref('stg_card_prices_snapshot') }}
),

cards as (
    select
        card_id,
        name,
        set_code,
        set_name,
        set_released_at,
        set_type,
        collector_number,
        rarity,
        rarity_rank,
        main_types,
        subtypes,
        color_names,
        cmc,
        layout
    from {{ ref('dim_cards') }}
)

select
    s.card_id,
    c.name,
    c.set_code,
    c.set_name,
    c.set_type,
    c.set_released_at,
    c.rarity,
    c.rarity_rank,
    c.collector_number,
    c.main_types,
    c.subtypes,
    c.color_names,
    c.cmc,
    c.layout,
    s.usd,
    s.usd_foil,
    s.eur,
    s.eur_foil,
    s.tix,
    s.dbt_valid_from as price_valid_from,
    s.dbt_valid_to as price_valid_to
from snapshot_data s
left join cards c on s.card_id = c.card_id
{% if is_incremental() %}
    where s.dbt_valid_from >= (select max(price_valid_from) from {{ this }})
{% endif %}
