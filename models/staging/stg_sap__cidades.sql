with
    fonte_cidades as (
        select
            cast(city as string) as nome_cidade
            , cast(stateprovinceid as int) as id_estado
        from {{ source('sap_adw', 'address') }}
    )
select *
from fonte_cidades