with type as (
    select *
      from {{ source( 'dbt_stock', 'yType') }}
)

select * from type