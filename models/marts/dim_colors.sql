with source as (
    select distinct
        color_initial,
        color_name
    from {{ ref('stg_card_colors') }}
    where color_initial is not null
),

colorless as (
    select
        null as color_initial,
        'Colorless' as color_name
)

select * from source
union
select * from colorless
