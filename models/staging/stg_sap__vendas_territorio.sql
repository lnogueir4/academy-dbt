with
    fonte_territorios as (
        select
            cast(territoryid as int) as id_territorio
            , cast(name as string) as nome_territorio
            , cast(countryregioncode as string) as codigo_pais
        from {{ source('sap_adw', 'salesterritory') }}
    )
select *
from fonte_territorios