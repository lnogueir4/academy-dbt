with
    stg_data_vendas as (
        select
            id_venda
            , data_venda
            , dia_mes_venda
            , nome_dia_venda
            , nome_mes_venda
            , nome_trimestre_venda
            , dia_ano_venda
            , ano_venda
        from {{ ref('stg_sap__data_vendas') }}
    )

select *
from stg_data_vendas