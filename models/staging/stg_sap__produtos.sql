with
    fonte_produtos as (
        select
            cast(productid as int) as id_produto
            , cast(name as string) as nome_produto
            , cast(productsubcategoryid as int) as id_pro_subcategoria
            , cast(standardcost as numeric) as custo_std_produto
            , cast(listprice as numeric) as preco_lista_produto
        from {{ source('sap_adw', 'product') }}
    )
select *
from fonte_produtos
