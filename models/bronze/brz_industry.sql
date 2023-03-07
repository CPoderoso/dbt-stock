with industry as (
    select *
      from {{ source( 'dtsc', 'yIndustry') }}
)

select * from industry