with
    fonte_pro_categorias as (
        select
            cast(productcategoryid as int) as id_pro_categoria
            , cast(name as string) as nome_pro_categoria
        from {{ source('sap_adw', 'productcategory') }}
    )
select *
from fonte_pro_categorias