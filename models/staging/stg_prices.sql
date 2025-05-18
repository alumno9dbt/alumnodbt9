with source as (
    select *
    from {{ source('bronze', 'card_prices') }}
),

cleaned as (
    select
        card_id,
        try_cast(usd as float) as usd,
        try_cast(usd_foil as float) as usd_foil,
        try_cast(eur as float) as eur,
        try_cast(eur_foil as float) as eur_foil,
        try_cast(tix as float) as tix
    from source
    where card_id is not null
)

select *
from cleaned
