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

    darm as (
        select *
        from {{ ref("staging_stu_darm_apropriacao") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify row_number() over (partition by darm order by data desc) = 1
    ),

    multa as (
        select *
        from {{ ref("staging_stu_multa") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),

    infracao_montado as (
        select
            m.data,
            m.datetime_captura as timestamp_captura,
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
            d.data_pagamento
        from multa m
        left join
            permissao p
            on m.tptran = cast(p.tptran as string)
            and m.tpperm = cast(p.tpperm as string)
            and m.termo = cast(p.termo as string)
        left join tipo_transporte t on m.tptran = t.id_tipo_transporte
        left join darm d on m.cm = d.darm
    ),

    dados_novos as (
        {% if is_incremental() %}
            select
                im.data as data_estado,
                coalesce(
                    im.timestamp_captura, da.timestamp_captura
                ) as timestamp_captura,
                coalesce(im.id_infracao, da.id_infracao) as id_infracao,
                coalesce(im.modo, da.modo) as modo,
                coalesce(im.servico, da.servico) as servico,
                coalesce(im.permissao, da.permissao) as permissao,
                coalesce(im.placa, da.placa) as placa,
                im.id_auto_infracao,
                coalesce(im.data_infracao, da.data_infracao) as data_infracao,
                coalesce(
                    im.datetime_infracao, da.datetime_infracao
                ) as datetime_infracao,
                coalesce(im.infracao, da.infracao) as infracao,
                coalesce(im.valor, da.valor) as valor,
                coalesce(im.status, da.status) as status,
                coalesce(im.data_pagamento, da.data_pagamento) as data_pagamento,
                '{{ var("version") }}' as versao,
                current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
                '{{ invocation_id }}' as id_execucao_dbt
            from infracao_montado im
            left join dados_atuais da using (id_auto_infracao)
        {% else %}
            select
                {{ data_v2_inicio }} as data_estado,
                im.* except (data),
                '{{ var("version") }}' as versao,
                current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
                '{{ invocation_id }}' as id_execucao_dbt
            from infracao_montado im
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
        partition by d.data, id_auto_infracao
        order by dc.data_estado desc, dc.ordem desc, dc.timestamp_captura desc
    )
    = 1
