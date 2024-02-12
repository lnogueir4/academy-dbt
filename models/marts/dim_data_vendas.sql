with
    stg_data_vendas as (
        select
            id_venda
            , data_venda
            , nome_do_dia
            , nome_do_mes
            , nome_trimestre
            , dia_do_ano
            , ano
        from {{ ref('stg_sap__data_vendas') }}
    )

select *
from stg_data_vendas