with credit as (
    select * from {{ source( 'dbt_stock', 'transactions') }}
     where type_id in (select type_id
                         from {{ source( 'dbt_stock', 'yType') }}
                        where type_transaction = 'C')
),

final as (
    select date_id as date_credit, symbol_id, shares, amount as credit
      from credit
)

select * from final