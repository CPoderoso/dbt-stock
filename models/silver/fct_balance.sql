{{
    config(
     materialized = 'incremental',
     unique_key = 'fct_balance_SK'
    )
}}

with transactions as (
    select symbol_id, date_credit as date_id, coalesce( shares, 0) as credit, 0 as debit 
        from {{ ref( 'fct_credit' ) }}
    union all
    SELECT symbol_id, date_debit as date_id, 0 as credit, shares as debit 
        from {{ ref( 'fct_debit' ) }}
),

balance as (
    select {{ dbt_utils.generate_surrogate_key( ['a.symbol_id', 'a.date_id'] ) }} as fct_balance_SK,
           a.symbol_id, a.date_id, a.close_vl * sum( b.credit - b.debit ) as balance_amt
      from {{ ref('fct_stockprice' ) }} a
      join transactions b 
        on ( a.symbol_id = b.symbol_id )
       and (a.date_id >= b.date_id)
       {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            where a.date_id > (select max(a.date_id) from {{ this }})
       {% endif %}
     group by a.symbol_id, a.date_id, a.close_vl
)

select * from balance
