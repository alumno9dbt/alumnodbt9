{{ config(materialized='table') }}

with source as (
    select distinct
        trim(f.value) as subtype
    from {{ ref('stg_card_types') }},
         lateral flatten(input => split(subtypes, ',')) as f
    where f.value is not null
)

select *
from source
