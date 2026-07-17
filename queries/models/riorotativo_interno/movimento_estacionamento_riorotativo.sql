{{
    config(
        materialized="incremental",
        alias="movimento_estacionamento",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set incremental_filter %}
    ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    and datetime_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

with
    movimento as (
        select *
        from {{ ref("staging_movimento_estacionamento_veiculo_riorotativo") }}
        where
            {{ incremental_filter }}
            and date(
                datetime_periodo_inicial
            ) between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
    ),
    estacionamento as (
        select *
        from {{ ref("staging_estacionamento_veiculo_riorotativo") }}
        where {{ incremental_filter }}
    ),
    veiculo_cliente as (
        select *
        from {{ ref("staging_veiculo_cliente_riorotativo") }}
        where {{ incremental_filter }}
    ),
    veiculo as (
        select *
        from {{ ref("staging_veiculo_riorotativo") }}
        where {{ incremental_filter }}
    ),
    movimento_completo as (
        select
            date(m.datetime_periodo_inicial) as data,
            m.id_movimento_estacionamento_veiculo,
            m.uuid_movimento_estacionamento_veiculo,
            m.id_estacionamento_veiculo,
            e.id_veiculo_cliente,
            vc.id_veiculo,
            v.placa as placa_veiculo,
            vc.cpf_motorista,
            e.latitude,
            e.longitude,
            st_geogpoint(e.longitude, e.latitude) as ponto_ativacao,
            a.area_codigo as id_area,
            a.area_perfil_funcionamento as ids_perfil_funcionamento,
            a.data_inicio_vigencia as data_inicio_vigencia_area,
            a.data_fim_vigencia as data_fim_vigencia_area,
            m.id_tipo_periodo,
            m.id_tipo_pagamento,
            m.datetime_periodo_inicial,
            m.datetime_periodo_final,
            m.datetime_pagamento,
            m.valor_periodo,
            m.valor_pago as valor_pago_bruto,
            round(m.valor_pago * numeric "0.042", 2) as valor_retido_jae,
            round(m.valor_pago * numeric "0.958", 2) as valor_pago_liquido,
            cast(null as numeric) as valor_acrescimo,
            cast(null as numeric) as valor_repasse_pcrj,
            round(m.valor_pago * numeric "0.042", 2) as valor_repasse_concessionaria,
            m.id_notificacao_veiculo,
            m.datetime_inclusao,
            m.datetime_captura,
            row_number() over (
                partition by m.id_movimento_estacionamento_veiculo
                order by a.data desc, a.data_inicio_vigencia desc, a.area_codigo
            ) as ordem_area
        from movimento as m
        left join
            estacionamento as e
            on m.id_estacionamento_veiculo = e.id_estacionamento_veiculo
        left join veiculo_cliente as vc on e.id_veiculo_cliente = vc.id_veiculo_cliente
        left join veiculo as v on vc.id_veiculo = v.id_veiculo
        left join
            {{ ref("staging_area_estacionamento_riorotativo") }} as a
            on st_covers(
                st_geogfromtext(a.area_poligono, make_valid => true),
                st_geogpoint(e.longitude, e.latitude)
            )
            and a.data <= date(m.datetime_periodo_inicial)
    ),
    movimento_area as (
        select * except (ordem_area) from movimento_completo where ordem_area = 1
    )
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from movimento_area
