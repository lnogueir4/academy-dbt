with
    fonte_oferta_especial as (
        select
            cast(specialofferid as int) as id_oferta_especial
            , cast(description as string) as descricao_oferta
            , cast(discountpct as numeric) as desconto_oferta
            , cast(type as string) as tipo_oferta
            , cast(category as string) as categoria_oferta
            --, cast(rowguid as int) as
            --, date(modifieddate) as
            -- colunas marcadas com comentario nao enra na regra de negocio
        from {{ source('sap_adw', 'specialoffer') }}
    )
select *
from fonte_oferta_especial