{{ config(materialized='view') }}
-- Apartment rent staging model

with source as (
    select * from {{ source('raw', 'apt_rent') }}
),

renamed as (
    select
        id,
        lawd_cd,
        deal_ymd,
        apt_name,
        umd_nm,
        region_name,

        -- price (unit: 10,000 KRW)
        deposit,
        monthly_rent,

        -- area / floor / year
        area_sqm,
        floor,
        build_year,

        -- contract date
        deal_year,
        deal_month,
        deal_day,

        -- derived columns
        (deal_year || '-' || lpad(deal_month::text, 2, '0')) as deal_ym,
        case
            when monthly_rent = 0 then 'jeonse'
            else                       'monthly'
        end as rent_type,
        case
            when area_sqm < 60  then 'small'
            when area_sqm < 85  then 'medium'
            when area_sqm < 135 then 'large'
            else                     'extra_large'
        end as size_category,

        created_at

    from source
)

select * from renamed