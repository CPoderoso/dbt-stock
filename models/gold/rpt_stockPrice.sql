
with rpt_stock as (
    select a.date_id, a.symbol_id, b.symbol_name, c.market_id, c.market_name, d.industry_id, d.industry_name,
           a.close_vl, a.high_vl, a.low_vl, a.open_vl, a.volume_amt, e.balance_amt
      from {{ ref('fct_stockprice') }} a
            inner join {{ ref('dim_symbol') }}  b on (a.symbol_id = b.symbol_id)
            inner join {{ ref('dim_market') }} c on (b.market_id = c.market_id)
            inner join {{ ref('dim_industry') }}  d on (b.industry_id = d.industry_id)
            inner join {{ ref('fct_balance') }} e on (a.symbol_id = e.symbol_id and a.date_id = e.date_id)
)

select * from rpt_stock