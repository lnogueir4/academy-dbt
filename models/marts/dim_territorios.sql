with
    stg_territorios as (
        select
            id_territorio
            , nome_territorio
            , codigo_pais
        from {{ ref('stg_sap__vendas_territorio') }}
    )

select *
from stg_territorios