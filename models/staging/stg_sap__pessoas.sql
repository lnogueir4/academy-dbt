with
    fonte_pessoas as (
        select
            cast(businessentityid as int) as id_pessoa
            , cast(firstname as string) || ' ' || cast(lastname as string) as nome_pessoa
        from {{ source('sap_adw', 'person') }}
    )
select *
from fonte_pessoas
