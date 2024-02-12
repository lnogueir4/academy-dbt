with
    fonte_produtos as (
        select
            cast(productid as int) as id_produto
            , cast(name as string) as nome_produto
        from {{ source('sap_adw', 'product') }}
    )
select *
from fonte_produtos
