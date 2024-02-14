with
    fonte_motivo_vendas as (
        select
            cast(salesreasonid as int) as id_motivo_venda
            , cast(name as string) as motivo_venda
            , cast(reasontype as string) as tipo_motivo
        from {{ source('sap_adw', 'salesreason') }}
    )
select *
from fonte_motivo_vendas
