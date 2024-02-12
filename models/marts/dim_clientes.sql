with
    stg_clientes as (
        select
            id_cliente
            , id_pessoa
        from {{ ref('stg_sap__clientes') }}
    )
    , stg_pessoas as (
        select
            id_pessoa
            , nome_cliente
        from {{ ref('stg_sap__pessoas') }}
    )
    , joined_tabelas as (
        select
            stg_clientes.id_cliente
            , stg_pessoas.id_pessoa          
            , stg_pessoas.nome_cliente 
        from stg_clientes
        left join stg_pessoas on
            stg_clientes.id_pessoa = stg_pessoas.id_pessoa
        
    )

select *
from joined_tabelas