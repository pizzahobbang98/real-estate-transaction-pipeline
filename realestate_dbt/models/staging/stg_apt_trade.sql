{{ config(materialized='view') }}
-- Apartment trade staging model
-- Only type conversion + column rename from source table


with source as (
    select * from {{ source('raw', 'apt_trade') }}
),

renamed as (
    select
        id,
        lawd_cd,
        deal_ymd,
        apt_name,
        umd_nm,
        region_name,

        -- trade price (unit: 10,000 KRW)
        deal_amount,

        -- area / floor / year
        area_sqm,
        floor,
        build_year,

        -- contract date
        deal_year,
        deal_month,
        deal_day,

        -- trade type
        dealing_gbn,
        cancel_deal,

        -- derived columns
        (deal_year || '-' || lpad(deal_month::text, 2, '0')) as deal_ym,
        case
            when area_sqm < 60  then 'small'
            when area_sqm < 85  then 'medium'
            when area_sqm < 135 then 'large'
            else                     'extra_large'
        end as size_category,

        created_at

    from source
    where cancel_deal is null 
       or cancel_deal = ''
       or cancel_deal = 'NaN'
)

select * from renamed