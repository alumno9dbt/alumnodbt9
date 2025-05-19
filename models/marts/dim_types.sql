with raw as (
    select
        trim(split_part(type.value::string, ',', 1)) as type
    from {{ ref('stg_card_types') }},
         lateral flatten(input => split(main_types, ', ')) as type
    where main_types is not null
)

select distinct type
from raw
where type != ''
