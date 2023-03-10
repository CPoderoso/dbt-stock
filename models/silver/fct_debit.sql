with debit as (
    select * from {{ ref( 'brz_transactions' )}}
     where type_id in (select type_id
                         from {{ ref( 'brz_type' )}}
                        where type_transaction = 'D')
),

final as (
    select date_id as date_debit, symbol_id, shares, amount as debit
      from debit
)

select * from final