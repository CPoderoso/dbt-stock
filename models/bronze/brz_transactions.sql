with transactions as (
    select *
      from {{ source( 'dtsc', 'yTransactions') }}
)

select * from transactions