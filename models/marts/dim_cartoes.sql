with
    stg_cartoes as (
        select
            id_cartao
            , bandeira_cartao
        from {{ ref('stg_sap__cartoes') }}
    )

select *
from stg_cartoes