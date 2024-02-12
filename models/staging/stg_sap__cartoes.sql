with
    fonte_cartoes as (
        select
            cast(creditcardid as int) as id_cartao
            , cast(cardtype as string) as bandeira_cartao
        from {{ source('sap_adw', 'creditcard') }}
    )
        
select *
from fonte_cartoes