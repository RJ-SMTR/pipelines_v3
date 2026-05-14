{{ config(materialized="ephemeral") }}

with
    veiculo_ativo as (select * from {{ ref("staging_stu_veiculo_ativo") }}),

    permissao as (
        select *
        from {{ ref("staging_stu_permissao") }}
        qualify
            row_number() over (
                partition by tptran, tpperm, termo, data order by data desc
            )
            = 1
    ),

    tipo_transporte as (
        select *
        from {{ ref("staging_stu_tipo_de_transporte") }}
        qualify
            row_number() over (partition by id_tipo_transporte, data order by data desc)
            = 1
    ),

    vistoria as (
        select *
        from {{ ref("staging_stu_vistoria") }}
        qualify
            row_number() over (partition by placa, data order by data_vistoria desc) = 1
    ),

    veiculo as (
        select *
        from {{ ref("staging_stu_veiculo") }}
        qualify row_number() over (partition by placa, data order by data desc) = 1
    ),

    planta as (
        select *
        from {{ ref("staging_stu_planta") }}
        qualify row_number() over (partition by id_planta, data order by data desc) = 1
    ),

    mod_carroceria as (
        select *
        from {{ ref("staging_stu_mod_carroceria") }}
        qualify
            row_number() over (
                partition by id_modelo_carroceria, data order by data desc
            )
            = 1
    ),

    mod_chassi as (
        select *
        from {{ ref("staging_stu_mod_chassi") }}
        qualify
            row_number() over (partition by id_modelo_chassi, data order by data desc)
            = 1
    ),

    combustivel as (
        select *
        from {{ ref("staging_stu_combustivel") }}
        qualify
            row_number() over (partition by id_combustivel, data order by data desc) = 1
    ),

    tipo_veiculo as (
        select *
        from {{ ref("staging_stu_tipo_de_veiculo") }}
        qualify
            row_number() over (partition by id_tipo_veiculo, data order by data desc)
            = 1
    )

select
    va.data,
    va.datetime_captura as timestamp_captura,
    t.descricao as modo,
    va.ordem as id_veiculo,
    safe_cast(ve.ano_fabricacao as int64) as ano_fabricacao,
    mc.descricao as carroceria,
    vi.data_vistoria as data_ultima_vistoria,
    safe_cast(pl.id_modelo_carroceria as int64) as id_carroceria,
    safe_cast(pl.id_modelo_chassi as int64) as id_chassi,
    safe_cast(mch.id_fabricante as int64) as id_fabricante_chassi,
    safe_cast(mc.id_fabricante as int64) as id_interno_carroceria,
    safe_cast(ve.id_planta as int64) as id_planta,
    pl.indicador_ar_condicionado,
    pl.indicador_deficiente_fisico as indicador_elevador,
    ve.indicador_usb,
    ve.indicador_wifi,
    mch.descricao as nome_chassi,
    concat(
        cast(p.tptran as string),
        cast(p.tpperm as string),
        '.',
        lpad(cast(p.termo as string), 6, '0'),
        '-',
        cast(p.dv as string)
    ) as permissao,
    va.placa,
    safe_cast(pl.lotacao_em_pe as int64) as quantidade_lotacao_pe,
    safe_cast(pl.lotacao_sentado as int64) as quantidade_lotacao_sentado,
    cb.descricao as tipo_combustivel,
    tv.descricao as tipo_veiculo,
    va.situacao as status,
    date(va.datetime_ativo) as data_inicio_vinculo,
    va.situacao as ultima_situacao,
    extract(year from vi.data_vistoria) as ano_ultima_vistoria
from veiculo_ativo va
left join
    permissao p
    on va.data = p.data
    and va.tptran = cast(p.tptran as string)
    and va.tpperm = cast(p.tpperm as string)
    and va.termo = cast(p.termo as string)
left join tipo_transporte t on va.tptran = t.id_tipo_transporte and va.data = t.data
left join vistoria vi on va.placa = vi.placa and va.data = vi.data
left join veiculo ve on va.placa = ve.placa and va.data = ve.data
left join planta pl on ve.id_planta = pl.id_planta and va.data = pl.data
left join
    mod_carroceria mc
    on pl.id_modelo_carroceria = mc.id_modelo_carroceria
    and va.data = mc.data
left join
    mod_chassi mch on pl.id_modelo_chassi = mch.id_modelo_chassi and va.data = mch.data
left join combustivel cb on ve.id_combustivel = cb.id_combustivel and va.data = cb.data
left join
    tipo_veiculo tv on pl.id_tipo_veiculo = tv.id_tipo_veiculo and va.data = tv.data
