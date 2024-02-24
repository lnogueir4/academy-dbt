with
    cartoes as (
        select *
        from {{ ref('dim_cartoes') }}
    )
    , cidades as (
        select *
        from {{ ref('dim_cidades') }}
    )
    , clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )
    , data_vendas as (
        select *
        from {{ ref('dim_data_vendas') }}
    )
    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )
    , vendedores as (
        select *
        from {{ ref('dim_vendedores') }}
    )
    , territorios as (
        select *
        from {{ ref('dim_territorios') }}
    )
    , int_vendas as (
        select *
            , cast(contagem_pedido as int) as conta_pedido
        from {{ ref('int_vendas__vendas_detalhes') }}
    )
    , joined_tabelas as (
        select
            int_vendas.sk_venda_detalhe
            , int_vendas.conta_pedido
            , int_vendas.linha_pedido
            , int_vendas.id_venda
            , int_vendas.data_venda
            , int_vendas.data_venct
            , int_vendas.data_envio
            , int_vendas.status_venda
            , int_vendas.eh_venda_online
            , int_vendas.id_cliente
            , int_vendas.id_vendedor
            , int_vendas.id_territorio
            , int_vendas.id_cidade
            , int_vendas.id_cartao
            , int_vendas.subtotal_venda
            , int_vendas.total_imposto
            , int_vendas.valor_frete
            , int_vendas.total_venda
            , int_vendas.id_venda_detalhe
            , int_vendas.qte_venda_detalhe
            , int_vendas.id_produto
            , int_vendas.preco_unitario
            , int_vendas.desc_perc_unitario
            , int_vendas.preco_total_bruto
            , int_vendas.preco_total_liquido
            , int_vendas.teve_desconto
            , int_vendas.qte_total_venda
            , int_vendas.id_oferta_especial
            , int_vendas.descricao_oferta
            , int_vendas.desconto_oferta
            , int_vendas.tipo_oferta
            , int_vendas.categoria_oferta
            --
            --, territorios.id_territorio
            , territorios.nome_territorio
            , territorios.codigo_pais
            --
            --, cartoes.id_cartao
            , cartoes.bandeira_cartao
            --
            --, cidades.id_cidade
            , cidades.nome_cidade
            , cidades.nome_estado
            , cidades.nome_pais
            --
            --, clientes.id_cliente
            , clientes.nome_cliente
            --
            , data_vendas.dia_mes_venda
            , data_vendas.nome_dia_venda
            , data_vendas.nome_mes_venda
            , data_vendas.nome_trimestre_venda
            , data_vendas.dia_ano_venda
            , data_vendas.ano_venda 
            --
            --, produtos.id_produto
            , produtos.nome_produto
            , produtos.nome_pro_subcategoria
            , produtos.nome_pro_categoria
            , int_vendas.custo_std_produto
            , int_vendas.custo_std_total_produto
            , int_vendas.preco_lista_produto
            , int_vendas.preco_lista_total_produto
            , int_vendas.lucro_previsto_cdesc
            , int_vendas.margem_bruta_prevista
            , int_vendas.lucro_obtido
            , int_vendas.margem_bruta_obtida
            , int_vendas.dif_margem
            , int_vendas.margem_status
            --
            --, vendedores.id_vendedor
            , vendedores.nome_vendedor
        from int_vendas
        left join cartoes on
            int_vendas.id_cartao =  cartoes.id_cartao
        left join cidades on
            int_vendas.id_cidade =  cidades.id_cidade
        left join clientes on
            int_vendas.id_cliente =  clientes.id_cliente
        left join data_vendas on
            int_vendas.id_venda =  data_vendas.id_venda
        left join produtos on
            int_vendas.id_produto =  produtos.id_produto
        left join vendedores on
            int_vendas.id_vendedor =  vendedores.id_vendedor
        left join territorios on
            int_vendas.id_territorio =  territorios.id_territorio
    )
    , final as (
        select
            sk_venda_detalhe
            , conta_pedido
            , id_venda
            , linha_pedido
            --
            , id_cliente
            , nome_cliente
            --
            , data_venda
            , data_venct
            , data_envio
            --
            , id_territorio
            , nome_territorio
            , codigo_pais
            , id_cidade
            , nome_cidade
            , nome_estado
            , nome_pais
            --
            , status_venda
            , eh_venda_online
            --
            , id_produto
            , nome_produto
            , nome_pro_subcategoria
            , nome_pro_categoria
            , custo_std_produto
            , custo_std_total_produto
            , preco_lista_produto
            , preco_lista_total_produto
            --
            , subtotal_venda
            , total_imposto
            , valor_frete
            , total_venda
            , id_venda_detalhe
            , qte_venda_detalhe
            , preco_unitario
            , preco_total_bruto
            , teve_desconto
            , desc_perc_unitario
            , preco_total_liquido
            , lucro_previsto_cdesc
            , margem_bruta_prevista
            , lucro_obtido
            , margem_bruta_obtida
            , margem_status
            , dif_margem
            , qte_total_venda
            , id_oferta_especial
            , descricao_oferta
            , desconto_oferta
            , tipo_oferta
            , categoria_oferta
            --
            , id_vendedor
            , nome_vendedor
            --
            , id_cartao
            , bandeira_cartao
            --
            , dia_mes_venda
            , nome_dia_venda
            , nome_mes_venda
            , nome_trimestre_venda
            , dia_ano_venda
            , ano_venda 

        from joined_tabelas
    )
    
select *
from final