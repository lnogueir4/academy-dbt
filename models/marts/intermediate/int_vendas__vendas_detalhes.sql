with
    venda_detalhes as (
        select *
        from {{ ref('stg_sap__venda_detalhes') }}
    )
    , vendas as (
        select *
        from {{ ref('stg_sap__vendas') }}
    )
    , joined_tabelas as (
        select
            vendas.id_venda
            , vendas.data_venda
            , vendas.data_venct
            , vendas.data_envio
            , vendas.status_venda
            , vendas.eh_venda_online
            , vendas.id_cliente
            , vendas.id_vendedor
            , vendas.id_cidade
            , vendas.id_cartao
            , vendas.subtotal_venda
            , vendas.total_imposto
            , vendas.valor_frete
            , vendas.total_venda
            , venda_detalhes.id_venda_detalhe
            , venda_detalhes.qte_venda_detalhe
            , venda_detalhes.id_produto
            , venda_detalhes.preco_unitario
            , venda_detalhes.desc_perc_unitario
        from venda_detalhes
        left join vendas on
            venda_detalhes.id_venda = vendas.id_venda
    )
    , metricas as (
        select
            cast(id_venda as string) || '-' || cast(id_venda_detalhe as string) as sk_venda_detalhe
            , dense_rank() over   (order by id_venda) as contagem_pedido
            , *
            , qte_venda_detalhe * preco_unitario as preco_total_bruto
            , qte_venda_detalhe * preco_unitario * (1 - desc_perc_unitario) as preco_total_liquido
            , case
                when desc_perc_unitario > 0 then 'Sim'
                else 'Nao'
            end as teve_desconto
            , sum   (qte_venda_detalhe) over(partition by id_venda) as qte_total_venda
        from joined_tabelas
    )
select *
from metricas
