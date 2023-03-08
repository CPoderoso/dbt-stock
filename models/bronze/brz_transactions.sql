with transactions as (
    select *
      from {{ source( 'dbt_stock', 'yTransactions') }}
)

select * from transactions