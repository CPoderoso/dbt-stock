with symbol as (
    select * from {{ref('brz_symbol')}}
)

select * from symbol