with symbol as (
    select *
      from {{ source( 'dtsc', 'ySymbol') }}
)

select * from symbol