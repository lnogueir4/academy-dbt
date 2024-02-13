with
    fonte_venda_detalhes as (
        select
            cast(salesorderid as int) as id_venda
            , cast(salesorderdetailid as int) as id_venda_detalhe
            --, cast(carriertrackingnumber as string) as
            , cast(orderqty as int) as qte_venda_detalhe
            , cast(productid as int) as id_produto
            --, cast(specialofferid as int) as id_oferta_especial
            , cast(unitprice as numeric) as preco_unitario
            , cast(unitpricediscount as numeric) as desc_perc_unitario
            --, cast(rowguid as int) as
            --, date(modifieddate) as data_modifica_venda_detalhe
            -- colunas marcadas com comentario nao enra na regra de negocio
        from {{ source('sap_adw', 'salesorderdetail') }}
    )
select *
from fonte_venda_detalhes 