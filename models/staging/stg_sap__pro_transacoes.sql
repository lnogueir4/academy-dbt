with
    fonte_transacoes as (
        select
            cast(transactionid as int) as id_transacao
            , cast(productid as int) as id_produto
            , cast(referenceorderid as int) as id_venda
            , cast(referenceorderlineid as int) as linha_pedido
            , date(transactiondate) as data_transacao
            --, cast(transactiontype as int) as
            , cast(quantity as int) as qte_transacao
            , cast(actualcost as numeric) as custo_atual_produto
        from {{ source('sap_adw', 'transactionhistory') }}
    )
select *
from fonte_transacoes