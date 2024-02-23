with
    venda_detalhes as (
        select *
        from {{ ref('stg_sap__venda_detalhes') }}
    )
    , vendas as (
        select *
        from {{ ref('stg_sap__vendas') }}
    )
    , oferta_especial as (
        select *
        from {{ ref('stg_sap__venda_ofertas_especiais') }}
    )
    , int_produto as (
        select *
        from {{ ref('stg_sap__produtos') }}
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
            , vendas.id_territorio
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
            , oferta_especial.id_oferta_especial
            , oferta_especial.descricao_oferta
            , oferta_especial.desconto_oferta
            , oferta_especial.tipo_oferta
            , oferta_especial.categoria_oferta
            , int_produto.custo_std_produto
            , int_produto.preco_lista_produto
        from venda_detalhes
        left join vendas on
            venda_detalhes.id_venda = vendas.id_venda
        left join oferta_especial on
            venda_detalhes.id_oferta_especial = oferta_especial.id_oferta_especial
        left join int_produto on
            venda_detalhes.id_produto = int_produto.id_produto
    )
    , metricas1 as (
        select
            dense_rank() over   (order by id_venda) as contagem_pedido
            , row_number() over   (partition by id_venda) as linha_pedido
            , *
            , qte_venda_detalhe * preco_unitario as preco_total_bruto
            , qte_venda_detalhe * preco_unitario * (1 - desc_perc_unitario) as preco_total_liquido
            , case
                when desc_perc_unitario > 0 then 'Sim'
                else 'Nao'
            end as teve_desconto
            , sum   (qte_venda_detalhe) over(partition by id_venda) as qte_total_venda
            , qte_venda_detalhe * custo_std_produto as custo_std_total_produto
            , qte_venda_detalhe * preco_lista_produto as preco_lista_total_produto
        from joined_tabelas
    )
    , metricas2 as (
        select
            cast(id_venda as string) || '-' || cast(linha_pedido as string) as sk_venda_detalhe
            , *
            , preco_lista_total_produto - custo_std_total_produto as lucro_previsto
            , preco_total_liquido - custo_std_total_produto as lucro_obtido
        from metricas1
    )
    , metricas3 as (
        select
            *
            , lucro_previsto / preco_lista_total_produto as margem_bruta_prevista
            , lucro_obtido / preco_total_liquido as margem_bruta_obtida
        from metricas2
    )
    , final as (
        select
            *
            , case
                when margem_bruta_obtida < 0 then 'Negativa'
                when margem_bruta_obtida <= 0.009 then 'Zero ou abaixo de 1%'
                when margem_bruta_obtida <= 0.05 then 'Positiva - 1% até 5%'
                when margem_bruta_obtida <= 0.15 then 'Positiva - 6% a 15%'
                when margem_bruta_obtida <= 0.25 then 'Positiva - 16% a 25%'
                when margem_bruta_obtida <= 0.35 then 'Positiva - 26% a 35%'
                else 'Acima de 35%'
            end as margem_status
            , margem_bruta_obtida - margem_bruta_prevista as dif_margem
            
        from metricas3
    )
    
select *
from final
