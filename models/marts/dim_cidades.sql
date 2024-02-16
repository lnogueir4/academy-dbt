with
    stg_pais as (
        select
            *
        from {{ ref('stg_sap__pais') }}
    )
    , stg_estados as (
        select
            *
        from {{ ref('stg_sap__estados') }}
    )
    , stg_cidades as (
        select
            *
        from {{ ref('stg_sap__cidades') }}
    )
    , joined_cidade_estado as (
        select
            stg_cidades.id_cidade
            , stg_cidades.nome_cidade          
            , stg_estados.nome_estado 
            , stg_estados.id_estado
            , stg_estados.id_pais
        from stg_cidades
        left join stg_estados on
            stg_cidades.id_estado = stg_estados.id_estado
    )
    , joined_cidade_pais as (
        select
            joined_cidade_estado.id_cidade
            , joined_cidade_estado.nome_cidade          
            , joined_cidade_estado.nome_estado 
            , joined_cidade_estado.id_estado
            , stg_pais.nome_pais
            , stg_pais.id_pais
        from joined_cidade_estado
        left join stg_pais on
            joined_cidade_estado.id_pais = stg_pais.id_pais
    )
select *
from joined_cidade_pais

