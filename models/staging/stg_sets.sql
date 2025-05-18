with source as (
    select *
    from {{ source('bronze', 'sets') }}
),

cleaned as (
    select
        set_code,
        name as set_name,
        try_to_date(released_at) as released_at,
        set_type
    from source
    where set_code is not null
)

select *
from cleaned
