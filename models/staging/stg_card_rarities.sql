with source as (
    select distinct
        lower(rarity) as rarity
    from {{ ref('base_cards') }}
    where rarity is not null
)

select
    rarity,
    case
        when rarity = 'common' then 1
        when rarity = 'uncommon' then 2
        when rarity = 'rare' then 3
        when rarity = 'mythic' then 4
        when rarity = 'special' then 5
        when rarity = 'bonus' then 6
        else 99
    end as rarity_rank
from source
