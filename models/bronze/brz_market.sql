with market as (
    select *
      from {{ source( 'dbt_stock', 'yMarket') }}
)

select * from market