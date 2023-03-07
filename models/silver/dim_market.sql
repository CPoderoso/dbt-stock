with market as (
    select * from {{ref('brz_market')}}
)

select * from market