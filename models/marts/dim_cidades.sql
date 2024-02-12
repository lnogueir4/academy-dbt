with
    stg_pais as (
        select
            id_pais
            , nome_pais
        from {{ ref('stg_sap__pais') }}
    )
    , stg_estados as (
        select
            id_estado
            , id_pais
            , nome_estado
        from {{ ref('stg_sap__estados') }}
    )
    , stg_cidades as (
        select
            id_cidade
            , id_estado
            , nome_cidade
        from {{ ref('stg_sap__cidades') }}
    )
    , joined_tabelas as (
        select
            stg_cidades.id_cidade
            , stg_cidades.nome_cidade          
            , stg_estados.nome_estado 
            , stg_pais.nome_pais
        from stg_estados
        left join stg_cidades on
            stg_estados.id_estado = stg_cidades.id_estado
        left join stg_pais on
            stg_estados.id_pais = stg_pais.id_pais
        
    )

select *
from joined_tabelas

