with
    /*selecao_distinto as (
        select distinct salesorderid, salesreasonid
        from {{ source('sap_adw', 'salesorderheadersalesreason') }}
    )*/
    fonte_motivo_id as (
        select
            cast(salesorderid as int) as id_venda
            , cast(salesreasonid as int) as id_motivo_venda
        from {{ source('sap_adw', 'salesorderheadersalesreason') }}
    )
select *
from fonte_motivo_id
