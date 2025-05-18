with cards as (
    select
        id as card_id,
        type_line
    from {{ ref('stg_cards') }}
    where type_line is not null
),

split_base as (
    select
        card_id,
        trim(split_part(type_line, '—', 1)) as main_type_block,
        trim(split_part(type_line, '—', 2)) as subtype_block
    from cards
),

main_types as (
    select
        sb.card_id,
        trim(mt.value::string) as main_type
    from split_base sb,
         lateral flatten(input => split(sb.main_type_block, ' ')) as mt
),

subtypes as (
    select
        sb.card_id,
        trim(st.value::string) as subtype
    from split_base sb,
         lateral flatten(input => split(sb.subtype_block, ' ')) as st
    where sb.subtype_block is not null
),

aggregated as (
    select
        m.card_id,
        array_agg(distinct m.main_type) as main_type_array,
        array_agg(distinct s.subtype) as subtype_array
    from main_types m
    left join subtypes s on m.card_id = s.card_id
    group by m.card_id
)

select
    card_id,
    array_to_string(main_type_array, ', ') as main_types,
    array_to_string(subtype_array, ', ') as subtypes
from aggregated
