with
    stg_clientes as (
        select
            *
        from {{ ref('stg_sap__clientes') }}
    )
    , stg_pessoas as (
        select
            *
        from {{ ref('stg_sap__pessoas') }}
    )
    , stg_lojas as (
        select
            *
        from {{ ref('stg_sap__lojas') }}
    )
    , joined_tabelas as (
        select
            stg_clientes.id_cliente
            , stg_clientes.id_territorio
            --, stg_pessoas.id_pessoa          
            , stg_pessoas.nome_pessoa as nome_cliente
            , stg_lojas.nome_loja
        from stg_clientes
        left join stg_pessoas on
            stg_clientes.id_pessoa = stg_pessoas.id_pessoa
        left join stg_lojas on
            stg_clientes.id_loja = stg_lojas.id_loja
        
    )
    , final as (
        select
            id_cliente
            , id_territorio        
            , nome_cliente
            , nome_loja
            , case
                when nome_loja is not null then 'Revenda'
                else 'Consumidor'
            end as tipo_cliente
        from joined_tabelas
           
    )
select *
from final