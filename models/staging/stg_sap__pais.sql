with
    fonte_pais as (
        select
            cast(countryregioncode as string) as id_pais
            , cast(name as string) as nome_pais
        from {{ source('sap_adw', 'countryregion') }}
    )
select *
from fonte_pais
