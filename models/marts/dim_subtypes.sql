with raw as (
    select
        trim(split_part(subtype.value::string, ',', 1)) as subtype
    from {{ ref('stg_card_types') }},
         lateral flatten(input => split(subtypes, ', ')) as subtype
    where subtypes is not null
)

select distinct subtype
from raw
where subtype != ''
