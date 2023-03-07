with Industry as (
    select * from {{ref('brz_industry')}}
)

select * from industry