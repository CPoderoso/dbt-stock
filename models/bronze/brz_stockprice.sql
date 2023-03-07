with stockprice as (
    select *
      from {{ source( 'dtsc', 'stock') }}
)

select * from stockprice