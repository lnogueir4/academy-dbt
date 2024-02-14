-- confirmar que a informação está de acordo com o levantado pela equipe de auditoria da contabilidade. 
-- vendas brutas no ano de 2011 foram de $12.646.112,16

with
    vendas_em_2011 as (
        select sum(preco_total_bruto) as total_bruto_vendido
        from {{ ref('fct_vendas') }}
        where data_venda between '2011-01-01' and '2011-12-31'
    )
select total_bruto_vendido
from vendas_em_2011
where total_bruto_vendido  not between 12646112 and 12646113
