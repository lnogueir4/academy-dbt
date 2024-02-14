with
    fonte_produtos as (
        select
            cast(productid as int) as id_produto
            , cast(name as string) as nome_produto
            , cast(productsubcategoryid as int) as id_pro_subcategoria
        from {{ source('sap_adw', 'product') }}
    )
select *
from fonte_produtos
