with industry as (
    select *
      from {{ source( 'dbt_stock', 'yIndustry') }}
)

select * from industry