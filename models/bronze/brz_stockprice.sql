with stockprice as (
    select *
      from {{ source( 'dbt_stock', 'stock') }}
)

select * from stockprice