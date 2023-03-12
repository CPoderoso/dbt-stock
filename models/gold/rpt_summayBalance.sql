with tmp_balance as (
    select a.symbol_id, sum( a.shares - coalesce(b.shares,0) ) as balance,
      from {{ ref( 'fct_credit')}} a left outer join {{ ref('fct_debit')}} b
        on (a.symbol_id = b.symbol_id)
     group by a.symbol_id
),

stockprice as (
    select a.date_id, a.symbol_id, a.close_vl * (
        select balance
          from tmp_balance b
         where b.symbol_id = a.symbol_id
        ) as amount_balance
       from {{ ref('fct_stockprice') }} a
      where a.date_id = (select max( date_id ) from {{ ref( 'fct_stockprice') }})
)

select * from stockprice