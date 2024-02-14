with
    fonte_vendas as(
        select
            cast(salesorderid as int) as id_venda
            --, cast(revisionnumber as int) as num_revisao_venda
            , date(orderdate) as data_venda
            , date(duedate) as data_venct
            , date(shipdate) as data_envio
            , cast(status as int) as status_venda
            , case
                when cast(onlineorderflag as string) = 'true' then 'Sim'
                else 'Nao'
            end as eh_venda_online
            --, cast(purchaseordernumber as string) as num_pedido_venda
            --, cast(accountnumber as string) as num_conta_venda
            , cast(customerid as int) as id_cliente
            , cast(salespersonid as int) as id_vendedor
            --, cast(territoryid as int) as id_territorio
            , cast(billtoaddressid as int) as id_cidade
            --, cast(shiptoaddressid as int) as id_cidade_envio
            --, cast(shipmethodid as int) as id_tipo_frete
            , cast(creditcardid as int) as id_cartao
            --, cast(creditcardapprovalcode as string) as id_cod_aprovacao
            --, cast(currencyrateid as int) as id_taxa_cambio
            , cast(subtotal as numeric) as subtotal_venda
            , cast(taxamt as numeric) as total_imposto
            , cast(freight as numeric) as valor_frete
            , cast(totaldue as numeric) as total_venda
            --, cast(comment as int) as
            --, cast(rowguid as int) as
            --, date(modifieddate) as data_modifica_venda
            -- colunas marcadas com comentario nao entra na regra de negocio
        from {{ source('sap_adw', 'salesorderheader') }}
    )

select *
from fonte_vendas

