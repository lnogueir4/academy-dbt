with
    fonte_data_vendas as (
        select
            cast(salesorderid as int) as id_venda
            , date(orderdate) as data_venda
            , extract(dayofweek from date(orderdate)) as dia_semana_venda
            , extract(month from date(orderdate)) as mes_venda
            , extract(quarter from date(orderdate)) as trimestre_venda
            , extract(dayofyear from date(orderdate)) as dia_ano_venda
            , extract(year from date(orderdate)) as ano_venda
            --, format_date('%p', date(orderdate)) as mes_ingles
            , format_date('%d-%m', date(orderdate)) as dia_mes_venda
        from {{ source('sap_adw', 'salesorderheader') }}
    )
    , data_nomes as (
        select *
            , case
                when dia_semana_venda = 1 then 'Domingo'
                when dia_semana_venda = 2 then 'Segunda-feira'
                when dia_semana_venda = 3 then 'Terça-feira'
                when dia_semana_venda = 4 then 'Quarta-feira'
                when dia_semana_venda = 5 then 'Quinta-feira'
                when dia_semana_venda = 6 then 'Sexta-feira'
                else 'Sábado' 
            end as nome_dia_venda
            , case
                when mes_venda = 1 then 'Janeiro'
                when mes_venda = 2 then 'Fevereiro'
                when mes_venda = 3 then 'Março'
                when mes_venda = 4 then 'Abril'
                when mes_venda = 5 then 'Maio'
                when mes_venda = 6 then 'Junho'
                when mes_venda = 7 then 'Julho'
                when mes_venda = 8 then 'Agosto'
                when mes_venda = 9 then 'Setembro'
                when mes_venda = 10 then 'Outubro'
                when mes_venda = 11 then 'Novembro'
                else 'Dezembro' 
            end as nome_mes_venda
            , case
                when trimestre_venda = 1 then '1º Trimestre'
                when trimestre_venda = 2 then '2º Trimestre'
                when trimestre_venda = 3 then '3º Trimestre'
                else '4º Trimestre' 
            end as nome_trimestre_venda
        from fonte_data_vendas
    )
            
    , final as (
        select 
            id_venda
            , data_venda
            --, dia_da_semana
            , dia_mes_venda
            , nome_dia_venda
            --, mes
            , nome_mes_venda
            --, trimestre
            , nome_trimestre_venda
            , dia_ano_venda
            , ano_venda
        from data_nomes
    )

select * 
from final
