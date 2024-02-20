with
    stg_produtos as (
        select
            *
        from {{ ref('stg_sap__produtos') }}
    )
    , stg_produtos_subcat as (
        select
            *
        from {{ ref('stg_sap__pro_subcategorias') }}
    )
    , stg_produtos_cat as (
        select
            *
        from {{ ref('stg_sap__pro_categorias') }}
    )
    , joined_tabelas as (
        select
            stg_produtos.id_produto
            , stg_produtos.nome_produto
            , stg_produtos.custo_std_produto
            , stg_produtos.preco_lista_produto
            , stg_produtos_subcat.id_pro_subcategoria
            , stg_produtos_subcat.nome_pro_subcategoria
            , stg_produtos_cat.id_pro_categoria
            , stg_produtos_cat.nome_pro_categoria
        from stg_produtos
        left join stg_produtos_subcat on
            stg_produtos.id_pro_subcategoria = stg_produtos_subcat.id_pro_subcategoria
        left join stg_produtos_cat on
            stg_produtos_subcat.id_pro_categoria = stg_produtos_cat.id_pro_categoria
    )
select *
from joined_tabelas