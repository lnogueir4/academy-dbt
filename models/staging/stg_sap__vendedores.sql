with
    fonte_vendedores as (
        select
            cast(businessentityid as int) as id_vendedor
            , cast(territoryid as int) as id_territorio
        from {{ source('sap_adw', 'salesperson') }}
    )
select *
from fonte_vendedores