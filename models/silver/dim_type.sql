with type as (
    select * from {{ref('brz_type')}}
)

select * from type