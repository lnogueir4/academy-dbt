with
    stg_motivo_id as (
        select distinct
            id_venda
            , id_motivo_venda
        from {{ ref('stg_sap__motivo_id') }}
    )
    , stg_motivo_vendas as (
        select
            id_motivo_venda
            , motivo_venda
        from {{ ref('stg_sap__motivo_vendas') }}
    )
    , joined_tabelas as (
        select
            stg_motivo_id.id_venda
            , stg_motivo_vendas.id_motivo_venda         
            , stg_motivo_vendas.motivo_venda
        from stg_motivo_id
        left join stg_motivo_vendas on
            stg_motivo_id.id_motivo_venda = stg_motivo_vendas.id_motivo_venda
        
    )
    /*, selecao_distintos as (
        select distinct id_venda
            --, id_motivo_venda
            --, motivo_venda
        from joined_tabelas
    )
    , pre_join as (
        select
            selecao_distintos.id_venda
            , stg_motivo_id.id_motivo_venda
        from selecao_distintos
        left join stg_motivo_id on
            selecao_distintos.id_venda = stg_motivo_id.id_venda
    )
    , final_join as (
        select
            pre_join.id_venda
            , pre_join.id_motivo_venda
            , stg_motivo_vendas.motivo_venda  
        from pre_join
        left join stg_motivo_vendas on
            pre_join.id_motivo_venda = stg_motivo_vendas.id_motivo_venda
    )*/
select *
from joined_tabelas