{% snapshot stg_card_prices_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='card_id',
        strategy='check',
        check_cols=['usd', 'usd_foil', 'eur', 'eur_foil', 'tix']
    )
}}

select * from {{ ref('stg_prices') }}

{% endsnapshot %}
