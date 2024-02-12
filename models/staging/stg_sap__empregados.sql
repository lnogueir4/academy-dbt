with
    fonte_empregado as (
        select
            cast(businessentityid as int) as id_empregado
        from {{ source('sap_adw', 'employee') }}
    )
select *
from fonte_empregado