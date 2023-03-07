with credit as (
    select * from {{ ref( 'brz_transactions' )}}
     where type_id in (select type_id
                         from {{ ref( 'brz_type' )}}
                        where tyep_transaction = 'C')
),

final as (
    select date_id as date_credit, symbol_id, shares, amount as credit
      from credit
)

select * from final