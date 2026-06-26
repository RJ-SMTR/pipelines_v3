{{ config(materialized="ephemeral") }}

with
    multa as (
        select *
        from {{ ref("staging_stu_multa") }}
        qualify row_number() over (partition by serie, cm order by data desc) = 1
    ),

    permissao as (
        select tptran, tpperm, termo, dv
        from {{ ref("staging_stu_permissao") }}
        qualify
            row_number() over (partition by tptran, tpperm, termo order by data desc)
            = 1
    ),

    tipo_transporte as (
        select *
        from {{ ref("staging_stu_tipo_de_transporte") }}
        qualify
            row_number() over (partition by id_tipo_transporte order by data desc) = 1
    ),

    darm as (
        select *
        from {{ ref("staging_stu_darm_apropriacao") }}
        qualify row_number() over (partition by darm, data order by data desc) = 1
    )

select
    m.data,
    m.id_infracao,
    t.descricao as modo,
    m.linha as servico,
    concat(
        cast(p.tptran as string),
        cast(p.tpperm as string),
        '.',
        lpad(cast(p.termo as string), 6, '0'),
        '-',
        cast(p.dv as string)
    ) as permissao,
    m.placa,
    concat(m.serie, '-', lpad(m.cm, 8, '0')) as id_auto_infracao,
    date(m.datetime_infracao) as data_infracao,
    m.datetime_infracao,
    m.descricao_infracao as infracao,
    m.valor,
    case
        when m.situacao = 'A'
        then 'Em Aberto'
        when m.situacao = 'C'
        then 'Cancelada'
        when m.situacao = 'PG'
        then 'Pago'
        when m.situacao = 'E'
        then 'Em Parcelamento'
        when m.situacao = 'PR'
        then 'Prescrita'
        when m.situacao = 'T'
        then 'Transferida'
        when m.situacao = 'PD'
        then 'Decurso de Prazo'
        else m.situacao
    end as status,
    d.data_pagamento,
    m.datetime_captura as timestamp_captura
from multa m
left join
    permissao p
    on m.tptran = cast(p.tptran as string)
    and m.tpperm = cast(p.tpperm as string)
    and m.termo = cast(p.termo as string)
left join tipo_transporte t on m.tptran = t.id_tipo_transporte
left join darm d on m.cm = d.darm and m.data = d.data
