with
    fonte_clientes as (
        select
            cast(customerid as int) as id_cliente
            , cast(personid as int) as id_pessoa
        from {{ source('sap_adw', 'customer') }}
    )
select *
from fonte_clientes
