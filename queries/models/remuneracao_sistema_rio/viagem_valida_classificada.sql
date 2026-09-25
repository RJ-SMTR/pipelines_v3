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

{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

{% set calendario = ref("calendario") %}
{% if execute %}
    {% set gtfs_feeds_query %}
        select distinct concat("'", feed_start_date, "'") as feed_start_date
        from {{ calendario }}
        where {{ incremental_filter }}
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% if gtfs_feeds | length == 0 %} {% set gtfs_feeds = ["'2000-01-01'"] %}
    {% endif %}
{% endif %}

with
    viagens as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            tipo_execucao_viagem,
            id_veiculo,
            placa,
            ano_fabricacao,
            servico,
            sentido,
            shape_id,
            distancia_planejada,
            modo,
            tipo_dia,
            tecnologia_apurada,
            tecnologia_remunerada,
            indicador_nao_licenciado,
            indicador_nao_vistoriado,
            indicador_lacrado,
            indicador_nao_autorizado_capacidade,
            indicador_autuado_ar_inoperante,
            indicador_autuado_alterar_itinerario,
            indicador_autuado_vista_inoperante,
            indicador_autuado_nao_atender_parada,
            indicador_autuado_nao_concluir_itinerario,
            indicador_registrado_ar_inoperante
        from {{ ref("aux_viagem_status") }}
        where {{ incremental_filter }}
    ),
    km_segmento as (
        select g.data, g.id_viagem, sum(s.comprimento_segmento) / 1000 as km_gps
        from
            (
                select distinct
                    data,
                    id_viagem,
                    feed_version,
                    feed_start_date,
                    shape_id,
                    id_segmento
                from {{ ref("gps_segmento_viagem") }}
                where {{ incremental_filter }} and quantidade_gps > 0
            ) as g
        inner join
            (
                select
                    feed_version,
                    feed_start_date,
                    shape_id,
                    id_segmento,
                    any_value(comprimento_segmento) as comprimento_segmento
                from {{ ref("segmento_shape") }}
                {% if execute %}
                    where feed_start_date in ({{ gtfs_feeds | join(", ") }})
                {% endif %}
                group by 1, 2, 3, 4
            ) as s
            on g.feed_version = s.feed_version
            and g.feed_start_date = s.feed_start_date
            and g.shape_id = s.shape_id
            and g.id_segmento = s.id_segmento
        group by 1, 2
    ),
    temperatura as (
        select *
        from {{ ref("viagem_valida_temperatura") }}
        where {{ incremental_filter }}
    ),
    bilhetagem as (
        select *
        from {{ ref("viagem_valida_bilhetagem") }}
        where {{ incremental_filter }}
    ),
    oferta as (
        select
            data,
            tipo_dia,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            lote
        from {{ ref("servico_oferta_faixa") }}
        where {{ incremental_filter }}
    ),
    tecnologia_servico as (
        select
            servico,
            inicio_vigencia,
            fim_vigencia,
            menor_tecnologia_permitida,
            maior_tecnologia_permitida
        from {{ ref("tecnologia_servico") }}
    ),
    base as (
        select
            v.data,
            v.id_viagem,
            v.datetime_partida,
            v.datetime_chegada,
            v.tipo_execucao_viagem,
            v.id_veiculo,
            v.placa,
            v.ano_fabricacao,
            v.servico,
            v.sentido,
            v.shape_id,
            v.distancia_planejada,
            v.modo,
            v.tipo_dia,
            coalesce(v.tecnologia_apurada, t.tecnologia_apurada) as tecnologia_apurada,
            v.tecnologia_remunerada,
            coalesce(v.indicador_nao_licenciado, false) as indicador_nao_licenciado,
            coalesce(v.indicador_nao_vistoriado, false) as indicador_nao_vistoriado,
            coalesce(v.indicador_lacrado, false) as indicador_lacrado,
            coalesce(
                v.indicador_nao_autorizado_capacidade, false
            ) as indicador_nao_autorizado_capacidade,
            coalesce(
                v.indicador_autuado_ar_inoperante, false
            ) as indicador_autuado_ar_inoperante,
            coalesce(
                v.indicador_autuado_alterar_itinerario, false
            ) as indicador_autuado_alterar_itinerario,
            coalesce(
                v.indicador_autuado_vista_inoperante, false
            ) as indicador_autuado_vista_inoperante,
            coalesce(
                v.indicador_autuado_nao_atender_parada, false
            ) as indicador_autuado_nao_atender_parada,
            coalesce(
                v.indicador_autuado_nao_concluir_itinerario, false
            ) as indicador_autuado_nao_concluir_itinerario,
            coalesce(
                v.indicador_registrado_ar_inoperante, false
            ) as indicador_registrado_ar_inoperante,
            coalesce(
                t.indicador_detectado_ar_inoperante, false
            ) as indicador_detectado_ar_inoperante,
            coalesce(
                t.indicador_regularidade_ar_condicionado_viagem, true
            ) as indicador_regularidade_ar_condicionado_viagem,
            coalesce(
                b.indicador_sem_transacao_tipo, false
            ) as indicador_sem_transacao_tipo,
            coalesce(
                b.indicador_validador_fechado, false
            ) as indicador_validador_fechado,
            coalesce(
                b.indicador_validador_associado_incorretamente, false
            ) as indicador_validador_associado_incorretamente
        from viagens v
        left join temperatura t using (data, id_viagem)
        left join bilhetagem b using (data, id_viagem)
    ),
    flag_status as (
        select b.data, b.id_viagem, f.impacto
        from base b
        cross join
            unnest(
                [
                    struct(
                        b.indicador_validador_associado_incorretamente as flag,
                        "invalida" as impacto
                    ),
                    struct(b.indicador_autuado_vista_inoperante, "invalida"),
                    struct(b.indicador_nao_licenciado, "invalida"),
                    struct(b.indicador_nao_vistoriado, "invalida"),
                    struct(b.indicador_lacrado, "invalida"),
                    struct(b.indicador_sem_transacao_tipo, "invalida"),
                    struct(b.indicador_validador_fechado, "invalida"),
                    struct(b.indicador_autuado_nao_atender_parada, "invalida"),
                    struct(b.indicador_autuado_alterar_itinerario, "invalida"),
                    struct(b.indicador_autuado_nao_concluir_itinerario, "invalida"),
                    struct(b.indicador_nao_autorizado_capacidade, "nao_conforme"),
                    struct(b.indicador_autuado_ar_inoperante, "nao_conforme"),
                    struct(b.indicador_registrado_ar_inoperante, "nao_conforme"),
                    struct(b.indicador_detectado_ar_inoperante, "nao_conforme")
                ]
            ) as f
        where f.flag
    ),
    fechamento as (
        select
            data,
            id_viagem,
            countif(impacto = "invalida") = 0 as indicador_viagem_valida,
            countif(impacto = "invalida") = 0
            and countif(impacto = "nao_conforme") = 0 as indicador_viagem_conforme
        from flag_status
        group by 1, 2
    ),
    com_oferta as (
        select
            b.*,
            o.tipo_dia as tipo_dia_oferta,
            o.lote,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            row_number() over (
                partition by b.data, b.id_viagem order by o.faixa_horaria_inicio
            ) as rn_faixa
        from base b
        left join
            oferta o
            on o.data = b.data
            and o.servico = b.servico
            and o.sentido = b.sentido
            and b.datetime_partida
            between o.faixa_horaria_inicio and o.faixa_horaria_fim
    ),
    carimbo as (select * except (rn_faixa) from com_oferta where rn_faixa = 1)
select
    concat(
        format_date('%Y-%m-%d', c.data),
        '|',
        c.lote,
        '|',
        c.id_veiculo,
        '|',
        format_datetime('%Y-%m-%dT%H:%M:%S', current_datetime("America/Sao_Paulo"))
    ) as id_apuracao,
    c.data,
    c.id_viagem,
    c.datetime_partida,
    c.datetime_chegada,
    ifnull(c.tipo_execucao_viagem, "COMPLETA") != "INCOMPLETA"
        as indicador_viagem_completa,
    coalesce(f.indicador_viagem_valida, true) as indicador_viagem_valida,
    coalesce(f.indicador_viagem_conforme, true) as indicador_viagem_conforme,
    cast(c.distancia_planejada as float64) as km_programada,
    cast(
        if(
            ifnull(c.tipo_execucao_viagem, "COMPLETA") = "INCOMPLETA",
            least(
                coalesce(k.km_gps, 0), coalesce(cast(c.distancia_planejada as float64), 0)
            ),
            cast(c.distancia_planejada as float64)
        ) as float64
    ) as km_percorrida,
    c.id_veiculo,
    c.placa,
    c.servico,
    c.sentido,
    c.shape_id,
    c.faixa_horaria_inicio,
    c.faixa_horaria_fim,
    ts.menor_tecnologia_permitida as tecnologia_minima_servico,
    ts.maior_tecnologia_permitida as tecnologia_maxima_servico,
    coalesce(c.tipo_dia_oferta, c.tipo_dia) as tipo_dia,
    json_object(
        'indicador_validador_associado_incorretamente',
        json_object('valor', c.indicador_validador_associado_incorretamente),
        'indicador_autuado_vista_inoperante',
        json_object('valor', c.indicador_autuado_vista_inoperante),
        'indicador_nao_licenciado',
        json_object('valor', c.indicador_nao_licenciado),
        'indicador_nao_vistoriado',
        json_object('valor', c.indicador_nao_vistoriado),
        'indicador_lacrado',
        json_object('valor', c.indicador_lacrado),
        'indicador_sem_transacao_tipo',
        json_object('valor', c.indicador_sem_transacao_tipo),
        'indicador_validador_fechado',
        json_object('valor', c.indicador_validador_fechado),
        'indicador_autuado_nao_atender_parada',
        json_object('valor', c.indicador_autuado_nao_atender_parada),
        'indicador_autuado_alterar_itinerario',
        json_object('valor', c.indicador_autuado_alterar_itinerario),
        'indicador_autuado_nao_concluir_itinerario',
        json_object('valor', c.indicador_autuado_nao_concluir_itinerario),
        'indicador_nao_autorizado_capacidade',
        json_object('valor', c.indicador_nao_autorizado_capacidade),
        'indicador_autuado_ar_inoperante',
        json_object('valor', c.indicador_autuado_ar_inoperante),
        'indicador_registrado_ar_inoperante',
        json_object('valor', c.indicador_registrado_ar_inoperante),
        'indicador_detectado_ar_inoperante',
        json_object('valor', c.indicador_detectado_ar_inoperante),
        'indicador_regularidade_ar_condicionado_viagem',
        json_object('valor', c.indicador_regularidade_ar_condicionado_viagem)
    ) as indicadores,
    c.tecnologia_apurada,
    coalesce(c.tecnologia_apurada, ts.menor_tecnologia_permitida) as tecnologia_fcf,
    c.tecnologia_remunerada,
    c.indicador_regularidade_ar_condicionado_viagem,
    c.modo,
    c.ano_fabricacao,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from carimbo c
left join fechamento f using (data, id_viagem)
left join km_segmento as k using (data, id_viagem)
left join
    tecnologia_servico ts
    on ts.servico = c.servico
    and (
        (c.data between ts.inicio_vigencia and ts.fim_vigencia)
        or (c.data >= ts.inicio_vigencia and ts.fim_vigencia is null)
    )
