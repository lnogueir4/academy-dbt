with
    stg_vendedores as (
        select
            id_vendedor
        from {{ ref('stg_sap__vendedores') }}
    )
    , stg_empregados as (
        select
            id_empregado
            , cargo
            , data_nasc
            , estado_civil
            , genero
            , data_admissao
            , horas_de_ferias
            , horas_de_afastamento
        from {{ ref('stg_sap__empregados') }}
    )
    , stg_pessoas as (
        select
            id_pessoa
            , nome_pessoa
        from {{ ref('stg_sap__pessoas') }}
    )
    , joined_tabelas as (
        select
            stg_vendedores.id_vendedor
            , stg_pessoas.nome_pessoa as nome_vendedor
            , stg_empregados.cargo
            , stg_empregados.data_nasc
            , stg_empregados.estado_civil
            , stg_empregados.genero
            , stg_empregados.data_admissao
            , stg_empregados.horas_de_ferias
            , stg_empregados.horas_de_afastamento    
        from stg_vendedores
        left join stg_empregados on
            stg_vendedores.id_vendedor = stg_empregados.id_empregado
        left join stg_pessoas on
            stg_vendedores.id_vendedor = stg_pessoas.id_pessoa
        
    )

select *
from joined_tabelas