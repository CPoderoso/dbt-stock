{{
    config(
        materialized='incremental',
        unique_key='stockprice_SK'
    )
}}

with stockprice as (
    select {{ dbt_utils.generate_surrogate_key( ['symbol_nm', 'date_id'] ) }} as stockprice_SK, *
      from {{ source( 'dbt_stock', 'stock') }}
      {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            where date_id > (select max(date_id) from {{ this }})

       {% endif %}
),

symbol as (
    select *
      from {{ source( 'dbt_stock', 'ySymbol') }}
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
 order by date_id DESC, symbol_id, stockprice_SK, close_vl
