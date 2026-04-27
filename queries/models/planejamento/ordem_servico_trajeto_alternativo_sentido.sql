{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

with
    ordem_servico_trajeto_alternativo as (
        select
            fi.feed_version,
            safe_cast(o.data_versao as date) feed_start_date,
            fi.feed_end_date,
            safe_cast(tipo_os as string) tipo_os,
            safe_cast(o.servico as string) servico,
            safe_cast(json_value(o.content, "$.ativacao") as string) ativacao,
            safe_cast(json_value(o.content, "$.consorcio") as string) consorcio,
            safe_cast(sentido as string) sentido,
            safe_cast(json_value(o.content, "$.extensao") as float64) extensao,
            safe_cast(evento as string) evento,
            safe_cast(json_value(o.content, "$.vista") as string) vista,
        from
            {{
                source(
                    "br_rj_riodejaneiro_gtfs_staging",
                    "ordem_servico_trajeto_alternativo_sentido",
                )
            }} o
        left join
            {{ ref("feed_info_gtfs") }} fi
            on o.data_versao = cast(fi.feed_start_date as string)
        {% if is_incremental() -%}
            where
                o.data_versao = "{{ var('data_versao_gtfs') }}"
                and fi.feed_start_date = "{{ var('data_versao_gtfs') }}"
        {%- endif %}
    )

select
    feed_version,
    feed_start_date,
    feed_end_date,
    tipo_os,
    servico,
    consorcio,
    sentido,
    vista,
    ativacao,
    case
        when evento like '[%]'
        then lower(evento)
        else regexp_replace(lower(evento), r"([a-záéíóúñüç]+)", r"[\1]")
    end as evento,
    extensao / 1000 as extensao,
    '{{ var("version") }}' as versao_modelo
from ordem_servico_trajeto_alternativo
