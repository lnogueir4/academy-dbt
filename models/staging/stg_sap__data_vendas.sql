with
    fonte_data_vendas as (
        select
            cast(salesorderid as int) as id_venda
            , date(orderdate) as data_venda
            , extract(dayofweek from date(orderdate)) as dia_da_semana
            , extract(month from date(orderdate)) as mes
            , extract(quarter from date(orderdate)) as trimestre
            , extract(dayofyear from date(orderdate)) as dia_do_ano
            , extract(year from date(orderdate)) as ano
            --, format_date('%p', date(orderdate)) as mes_ingles
            , format_date('%d-%m', date(orderdate)) as dia_mes
        from {{ source('sap_adw', 'salesorderheader') }}
    )
    , data_nomes as (
        select *
            , case
                when dia_da_semana = 1 then 'Domingo'
                when dia_da_semana = 2 then 'Segunda-feira'
                when dia_da_semana = 3 then 'Terça-feira'
                when dia_da_semana = 4 then 'Quarta-feira'
                when dia_da_semana = 5 then 'Quinta-feira'
                when dia_da_semana = 6 then 'Sexta-feira'
                else 'Sábado' 
            end as nome_do_dia
            , case
                when mes = 1 then 'Janeiro'
                when mes = 2 then 'Fevereiro'
                when mes = 3 then 'Março'
                when mes = 4 then 'Abril'
                when mes = 5 then 'Maio'
                when mes = 6 then 'Junho'
                when mes = 7 then 'Julho'
                when mes = 8 then 'Agosto'
                when mes = 9 then 'Setembro'
                when mes = 10 then 'Outubro'
                when mes = 11 then 'Novembro'
                else 'Dezembro' 
            end as nome_do_mes
            , case
                when trimestre = 1 then '1º Trimestre'
                when trimestre = 2 then '2º Trimestre'
                when trimestre = 3 then '3º Trimestre'
                else '4º Trimestre' 
            end as nome_trimestre
        from fonte_data_vendas
    )
            
    , final_cte as (
        select 
            id_venda
            , data_venda
            --, dia_da_semana
            , nome_do_dia
            --, mes
            , nome_do_mes
            --, trimestre
            , nome_trimestre
            , dia_do_ano
            , ano
        from data_nomes
    )

select * 
from final_cte
