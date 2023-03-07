with market as (
    select *
      from {{ source( 'dtsc', 'yMarket') }}
)

select * from market