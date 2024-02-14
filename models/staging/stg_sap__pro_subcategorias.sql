with
    fonte_pro_subcategorias as (
        select
            cast(productsubcategoryid as int) as id_pro_subcategoria
            , cast(productcategoryid as int) as id_pro_categoria
            , cast(name as string) as nome_pro_subcategoria
        from {{ source('sap_adw', 'productsubcategory') }}
    )
select *
from fonte_pro_subcategorias