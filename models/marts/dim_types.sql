{{ config(materialized='table') }}

with source as (
    select distinct
        trim(f.value) as main_type
    from {{ ref('stg_card_types') }},
         lateral flatten(input => split(main_types, ',')) as f
    where f.value is not null
)

select *
from source
