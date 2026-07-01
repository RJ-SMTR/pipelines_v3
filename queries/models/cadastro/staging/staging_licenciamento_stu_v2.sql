{{ config(materialized="ephemeral") }}

with
    veiculo_ativo as (select * from {{ ref("veiculo_ativo") }}),

    permissao as (select tptran, tpperm, termo, dv from {{ ref("permissao") }}),

    tipo_transporte as (select * from {{ ref("tipo_de_transporte") }}),

    vistoria as (select * from {{ ref("vistoria") }}),

    veiculo as (select * from {{ ref("veiculo") }}),

    planta as (select * from {{ ref("planta") }}),

    mod_carroceria as (select * from {{ ref("mod_carroceria") }}),

    mod_chassi as (select * from {{ ref("mod_chassi") }}),

    combustivel as (select * from {{ ref("combustivel") }}),

    tipo_veiculo as (select * from {{ ref("tipo_de_veiculo") }})

select
    coalesce(date(va.datetime_captura), vi.data) as data,
    va.datetime_captura as timestamp_captura,
    t.descricao as modo,
    trim(coalesce(vi.ordem, va.ordem)) as id_veiculo,
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
    coalesce(vi.placa, va.placa) as placa,
    safe_cast(pl.lotacao_em_pe as int64) as quantidade_lotacao_pe,
    safe_cast(pl.lotacao_sentado as int64) as quantidade_lotacao_sentado,
    cb.descricao as tipo_combustivel,
    tv.descricao as tipo_veiculo,
    'Licenciado' as status,
    date(va.datetime_ativo) as data_inicio_vinculo,
    case
        when coalesce(va.situacao, vi.situacao) = 'N'
        then 'Normal'
        when coalesce(va.situacao, vi.situacao) = 'S'
        then 'Suspenso'
        when coalesce(va.situacao, vi.situacao) = 'C'
        then 'Cancelado'
        else coalesce(va.situacao, vi.situacao)
    end as ultima_situacao,
    extract(year from vi.data_vistoria) as ano_ultima_vistoria
from veiculo_ativo va
left join
    vistoria vi
    on va.tptran = vi.tptran
    and va.tpperm = vi.tpperm
    and va.termo = vi.termo
    and va.placa = vi.placa
left join
    permissao p
    on coalesce(vi.tptran, va.tptran) = cast(p.tptran as string)
    and coalesce(vi.tpperm, va.tpperm) = cast(p.tpperm as string)
    and coalesce(vi.termo, va.termo) = cast(p.termo as string)
left join tipo_transporte t on coalesce(vi.tptran, va.tptran) = t.id_tipo_transporte
left join veiculo ve on coalesce(vi.placa, va.placa) = ve.placa
left join
    planta pl on ve.id_planta = pl.id_planta and ve.id_tipo_veiculo = pl.id_tipo_veiculo
left join mod_carroceria mc on pl.id_modelo_carroceria = mc.id_modelo_carroceria
left join mod_chassi mch on pl.id_modelo_chassi = mch.id_modelo_chassi
left join combustivel cb on ve.id_combustivel = cb.id_combustivel
left join tipo_veiculo tv on ve.id_tipo_veiculo = tv.id_tipo_veiculo
