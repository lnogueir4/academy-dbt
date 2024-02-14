with
    fonte_empregado as (
        select
            cast(businessentityid as int) as id_empregado
            , cast(jobtitle as string) as cargo
            , date(birthdate) as data_nasc
            , cast(maritalstatus as string) as estado_civil
            , cast(gender as string) as genero
            , date(hiredate) as data_admissao
            , cast(vacationhours as int) as horas_de_ferias
            , cast(sickleavehours as int) as horas_de_afastamento
        from {{ source('sap_adw', 'employee') }}
    )
select *
from fonte_empregado