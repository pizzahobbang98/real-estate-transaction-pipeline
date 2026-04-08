{{ config(materialized='table') }}

with trade_stats as (
    select * from {{ ref('int_apt_monthly_stats') }}
),

rent as (
    select
        region_name,
        deal_ym,
        size_category,
        rent_type,
        count(*)            as rent_count,
        avg(deposit)        as avg_deposit,
        avg(monthly_rent)   as avg_monthly_rent
    from {{ ref('stg_apt_rent') }}
    group by region_name, deal_ym, size_category, rent_type
),

jeonse as (
    select
        region_name,
        deal_ym,
        size_category,
        avg_deposit         as avg_jeonse_price,
        rent_count          as jeonse_count
    from rent
    where rent_type = 'jeonse'
)

select
    t.region_name,
    t.deal_ym,
    t.size_category,

    -- trade info
    t.trade_count,
    t.avg_trade_price,
    t.min_trade_price,
    t.max_trade_price,
    t.avg_area_sqm,

    -- jeonse info
    j.jeonse_count,
    round(j.avg_jeonse_price::numeric, 0)   as avg_jeonse_price,

    -- jeonse ratio (jeonse / trade price)
    case
        when t.avg_trade_price > 0
        then round((j.avg_jeonse_price / t.avg_trade_price * 100)::numeric, 1)
        else null
    end                                     as jeonse_ratio

from trade_stats t
left join jeonse j
    on  t.region_name   = j.region_name
    and t.deal_ym       = j.deal_ym
    and t.size_category = j.size_category