{{ config(materialized='view') }}

with cards as (
    select
        id as card_id,
        split_part(type_line, ' // ', 1) as type_line_clean
    from {{ ref('base_cards') }}
    where type_line is not null
),

split_base as (
    select
        card_id,
        trim(split_part(type_line_clean, '—', 1)) as main_type_block,
        trim(split_part(type_line_clean, '—', 2)) as subtype_block
    from cards
),

main_types as (
    select
        card_id,
        array_sort(array_distinct(split(main_type_block, ' '))) as main_type_array
    from split_base
),

subtypes as (
    select
        card_id,
        array_sort(array_distinct(split(subtype_block, ' '))) as subtype_array
    from split_base
    where subtype_block is not null
),

joined as (
    select
        coalesce(m.card_id, s.card_id) as card_id,
        coalesce(m.main_type_array, []) as main_type_array,
        coalesce(s.subtype_array, []) as subtype_array
    from main_types m
    full outer join subtypes s
    on m.card_id = s.card_id
)

select
    card_id,
    array_to_string(main_type_array, ', ') as main_types,
    array_to_string(subtype_array, ', ') as subtypes
from joined
