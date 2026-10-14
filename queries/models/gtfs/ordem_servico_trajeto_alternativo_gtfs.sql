{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        alias="ordem_servico_trajeto_alternativo",
    )
}}

with
    ordem_servico_trajeto_alternativo as (
        select
            fi.feed_version,
            o.data_versao feed_start_date,
            fi.feed_end_date,
            safe_cast(tipo_os as string) tipo_os,
            safe_cast(evento as string) evento,
            safe_cast(o.servico as string) servico,
            safe_cast(json_value(o.content, "$.ativacao") as string) ativacao,
            safe_cast(json_value(o.content, "$.consorcio") as string) consorcio,
            safe_cast(json_value(o.content, "$.descricao") as string) descricao,
            safe_cast(json_value(o.content, "$.extensao_ida") as float64) extensao_ida,
            safe_cast(
                json_value(o.content, "$.extensao_volta") as float64
            ) extensao_volta,
            safe_cast(
                json_value(o.content, "$.horario_inicio") as string
            ) horario_inicio,
            safe_cast(json_value(o.content, "$.horario_fim") as string) horario_fim,
            safe_cast(json_value(o.content, "$.vista") as string) vista,
        from
            {{
                source(
                    "source_gtfs",
                    "ordem_servico_trajeto_alternativo",
                )
            }} o
        left join {{ ref("feed_info_gtfs") }} fi on o.data_versao = fi.feed_start_date
        where
            o.data_versao < date("{{ var('DATA_GTFS_V5_INICIO') }}")
            {% if is_incremental() -%}
                and o.data_versao = date("{{ var('data_versao_gtfs') }}")
                and date(fi.feed_start_date) = date("{{ var('data_versao_gtfs') }}")
            {%- endif %}
    )

select
    feed_version,
    feed_start_date,
    feed_end_date,
    tipo_os,
    servico,
    consorcio,
    vista,
    ativacao,
    descricao,
    case
        when evento like '[%]'
        then lower(evento)
        else regexp_replace(lower(evento), r"([a-záéíóúñüç]+)", r"[\1]")
    end as evento,
    extensao_ida / 1000 as extensao_ida,
    extensao_volta / 1000 as extensao_volta,
    horario_inicio as inicio_periodo,
    horario_fim as fim_periodo,
    '{{ var("version") }}' as versao_modelo
from ordem_servico_trajeto_alternativo
