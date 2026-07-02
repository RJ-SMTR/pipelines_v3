{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set data_v2_inicio %}
    date_add(date("{{ var('data_inicio_dbstu') }}"), interval 1 day)
{% endset %}

{% set incremental_filter %}
    data between greatest(
        date("{{ var('date_range_start') }}"), {{ data_v2_inicio }}
    ) and date("{{ var('date_range_end') }}")
{% endset %}

with
    datas as (
        select data
        from
            unnest(
                generate_date_array(
                    {% if is_incremental() %}
                        greatest(
                            date("{{ var('date_range_start') }}"), {{ data_v2_inicio }}
                        ),
                        date("{{ var('date_range_end') }}")
                    {% else %}{{ data_v2_inicio }}, {{ data_v2_inicio }}
                    {% endif %}
                )
            ) as data
    ),

    {% if is_incremental() %}
        dados_atuais as (
            select data as data_estado, * except (data)
            from {{ this }}
            where
                data = date_sub(
                    greatest(
                        date("{{ var('date_range_start') }}"), {{ data_v2_inicio }}
                    ),
                    interval 1 day
                )
        ),
    {% endif %}

    permissao as (
        select tptran, tpperm, termo, dv
        from {{ ref("staging_stu_permissao") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (partition by tptran, tpperm, termo order by data desc)
            = 1
    ),

    tipo_transporte as (
        select *
        from {{ ref("staging_stu_tipo_de_transporte") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (partition by id_tipo_transporte order by data desc) = 1
    ),

    vistoria as (
        select *
        from {{ ref("staging_stu_vistoria") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by placa, tptran, tpperm, termo order by data_vistoria desc
            )
            = 1
    ),

    veiculo as (
        select *
        from {{ ref("staging_stu_veiculo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify row_number() over (partition by placa order by data desc) = 1
    ),

    planta as (
        select *
        from {{ ref("staging_stu_planta") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by id_planta, id_tipo_veiculo order by data desc
            )
            = 1
    ),

    mod_carroceria as (
        select *
        from {{ ref("staging_stu_mod_carroceria") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (partition by id_modelo_carroceria order by data desc) = 1
    ),

    mod_chassi as (
        select *
        from {{ ref("staging_stu_mod_chassi") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify row_number() over (partition by id_modelo_chassi order by data desc) = 1
    ),

    combustivel as (
        select *
        from {{ ref("staging_stu_combustivel") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify row_number() over (partition by id_combustivel order by data desc) = 1
    ),

    tipo_veiculo as (
        select *
        from {{ ref("staging_stu_tipo_de_veiculo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify row_number() over (partition by id_tipo_veiculo order by data desc) = 1
    ),

    veiculo_ativo as (
        select *
        from {{ ref("staging_stu_veiculo_ativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),

    licenciamento_montado as (
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
        left join
            tipo_transporte t on coalesce(vi.tptran, va.tptran) = t.id_tipo_transporte
        left join veiculo ve on coalesce(vi.placa, va.placa) = ve.placa
        left join
            planta pl
            on ve.id_planta = pl.id_planta
            and ve.id_tipo_veiculo = pl.id_tipo_veiculo
        left join mod_carroceria mc on pl.id_modelo_carroceria = mc.id_modelo_carroceria
        left join mod_chassi mch on pl.id_modelo_chassi = mch.id_modelo_chassi
        left join combustivel cb on ve.id_combustivel = cb.id_combustivel
        left join tipo_veiculo tv on ve.id_tipo_veiculo = tv.id_tipo_veiculo
    ),

    dados_novos as (
        {% if is_incremental() %}
            select
                lm.data as data_estado,
                coalesce(
                    lm.timestamp_captura, da.timestamp_captura
                ) as timestamp_captura,
                coalesce(lm.modo, da.modo) as modo,
                lm.id_veiculo,
                coalesce(lm.ano_fabricacao, da.ano_fabricacao) as ano_fabricacao,
                coalesce(lm.carroceria, da.carroceria) as carroceria,
                coalesce(
                    lm.data_ultima_vistoria, da.data_ultima_vistoria
                ) as data_ultima_vistoria,
                coalesce(lm.id_carroceria, da.id_carroceria) as id_carroceria,
                coalesce(lm.id_chassi, da.id_chassi) as id_chassi,
                coalesce(
                    lm.id_fabricante_chassi, da.id_fabricante_chassi
                ) as id_fabricante_chassi,
                coalesce(
                    lm.id_interno_carroceria, da.id_interno_carroceria
                ) as id_interno_carroceria,
                coalesce(lm.id_planta, da.id_planta) as id_planta,
                coalesce(
                    lm.indicador_ar_condicionado, da.indicador_ar_condicionado
                ) as indicador_ar_condicionado,
                coalesce(
                    lm.indicador_elevador, da.indicador_elevador
                ) as indicador_elevador,
                coalesce(lm.indicador_usb, da.indicador_usb) as indicador_usb,
                coalesce(lm.indicador_wifi, da.indicador_wifi) as indicador_wifi,
                coalesce(lm.nome_chassi, da.nome_chassi) as nome_chassi,
                coalesce(lm.permissao, da.permissao) as permissao,
                coalesce(lm.placa, da.placa) as placa,
                coalesce(
                    lm.quantidade_lotacao_pe, da.quantidade_lotacao_pe
                ) as quantidade_lotacao_pe,
                coalesce(
                    lm.quantidade_lotacao_sentado, da.quantidade_lotacao_sentado
                ) as quantidade_lotacao_sentado,
                coalesce(lm.tipo_combustivel, da.tipo_combustivel) as tipo_combustivel,
                coalesce(lm.tipo_veiculo, da.tipo_veiculo) as tipo_veiculo,
                coalesce(lm.status, da.status) as status,
                coalesce(
                    lm.data_inicio_vinculo, da.data_inicio_vinculo
                ) as data_inicio_vinculo,
                coalesce(lm.ultima_situacao, da.ultima_situacao) as ultima_situacao,
                coalesce(
                    lm.ano_ultima_vistoria, da.ano_ultima_vistoria
                ) as ano_ultima_vistoria,
                '{{ var("version") }}' as versao,
                current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
                '{{ invocation_id }}' as id_execucao_dbt
            from licenciamento_montado lm
            left join dados_atuais da using (id_veiculo)
        {% else %}
            select
                {{ data_v2_inicio }} as data_estado,
                lm.* except (data),
                '{{ var("version") }}' as versao,
                current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
                '{{ invocation_id }}' as id_execucao_dbt
            from licenciamento_montado lm
        {% endif %}
    ),

    dados_completos as (
        {% if is_incremental() %}
            select *, 0 as ordem
            from dados_atuais
            union all by name
        {% endif %}
        select *, 1 as ordem
        from dados_novos
    )

select d.data, dc.* except (data_estado, ordem)
from datas d
join dados_completos dc on dc.data_estado <= d.data
qualify
    row_number() over (
        partition by d.data, id_veiculo
        order by dc.data_estado desc, dc.ordem desc, dc.timestamp_captura desc
    )
    = 1
