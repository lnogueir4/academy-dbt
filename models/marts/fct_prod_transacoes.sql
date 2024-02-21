with
    fct_transacao as (
        select
            id_transacao
            , id_produto
            , id_venda
            , linha_pedido
            , data_transacao
            , qte_transacao
            , custo_atual_produto
            , 
        from {{ ref('stg_sap__pro_transacoes') }}
    )

select *
from fct_transacao