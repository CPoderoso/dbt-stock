with symbol as (
    select *
      from {{ source( 'dbt_stock', 'ySymbol') }}
)

select * from symbol