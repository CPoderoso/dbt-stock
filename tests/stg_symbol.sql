with symbols as (
    select 
     distinct symbol_nm, date_id 
     from  {{ source( 'dtsc', 'stock') }}
),

final as (
    select distinct symbol_nm as symbol, row_number() 
        over( partition by date_id ) as symbol_id
      from symbols
)

select * from final