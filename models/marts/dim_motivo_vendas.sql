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
            , tipo_motivo
        from {{ ref('stg_sap__motivo_vendas') }}
    )
    , joined_tabelas as (
        select
            stg_motivo_id.id_venda
            , stg_motivo_vendas.id_motivo_venda         
            , stg_motivo_vendas.motivo_venda
            , stg_motivo_vendas.tipo_motivo
        from stg_motivo_id
        left join stg_motivo_vendas on
            stg_motivo_id.id_motivo_venda = stg_motivo_vendas.id_motivo_venda
    )
select *
from joined_tabelas