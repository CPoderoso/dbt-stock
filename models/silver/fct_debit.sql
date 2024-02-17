with debit as (
    select * from {{ source( 'dbt_stock', 'transactions') }}
     where type_id in (select type_id
                         from {{ source( 'dbt_stock', 'yType') }}
                        where type_transaction = 'D')
),

final as (
    select date_id as date_debit, symbol_id, shares, amount as debit
      from debit
)

select * from final