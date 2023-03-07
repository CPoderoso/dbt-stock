with type as (
    select *
      from {{ source( 'dtsc', 'yType') }}
)

select * from type