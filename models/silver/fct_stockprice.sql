with stockprice as (
    select {{ dbt_utils.generate_surrogate_key( ['symbol_nm', 'date_id'] ) }} as stockprice_SK, *
      from {{ ref('brz_stockprice')}}
),

symbol as (
    select *
      from {{ ref('brz_symbol')}}
),

final as (
    select stockprice_SK, date_id,
        (
            select symbol_id
              from symbol b
             where a.symbol_nm = b.symbol_name
        ) as symbol_id,
        close_vl, dividends_vl, high_vl, low_vl, open_vl, stock_splits, volume_amt
      from stockprice a
)

select * from final
