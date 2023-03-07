with tmp_balance as (
    select c.industry_id, a.symbol_id, sum( a.shares - coalesce(b.shares,0) ) as balance,
      from {{ ref('dim_symbol')}} c 
            left outer join {{ ref( 'fct_credit')}} a on (a.symbol_id = c.symbol_id)
            left outer join {{ ref('fct_debit')}} b on (a.symbol_id = b.symbol_id)
     group by c.industry_id, a.symbol_id
),

stockprice as (
    select a.date_id, b.industry_id, sum(a.close_vl * b.balance ) as amount_balance
       from {{ ref('fct_stockprice') }} a
            left outer join tmp_balance b on (a.symbol_id = b.symbol_id)
      where a.date_id = (select max( date_id ) from {{ ref( 'fct_stockprice') }})
      group by a.date_id, b.industry_id
)

select * from stockprice