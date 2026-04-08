{{ config(materialized='view') }}

with trade as (
    select * from {{ ref('stg_apt_trade') }}
),

rent as (
    select * from {{ ref('stg_apt_rent') }}
),

trade_stats as (
    select
        region_name,
        deal_ym,
        size_category,
        count(*)                    as trade_count,
        avg(deal_amount)            as avg_trade_price,
        min(deal_amount)            as min_trade_price,
        max(deal_amount)            as max_trade_price,
        avg(area_sqm)               as avg_area_sqm
    from trade
    group by region_name, deal_ym, size_category
),

rent_stats as (
    select
        region_name,
        deal_ym,
        size_category,
        rent_type,
        count(*)                    as rent_count,
        avg(deposit)                as avg_deposit,
        avg(monthly_rent)           as avg_monthly_rent
    from rent
    group by region_name, deal_ym, size_category, rent_type
)

select
    t.region_name,
    t.deal_ym,
    t.size_category,
    t.trade_count,
    round(t.avg_trade_price::numeric, 0)    as avg_trade_price,
    round(t.min_trade_price::numeric, 0)    as min_trade_price,
    round(t.max_trade_price::numeric, 0)    as max_trade_price,
    round(t.avg_area_sqm::numeric, 2)       as avg_area_sqm
from trade_stats t