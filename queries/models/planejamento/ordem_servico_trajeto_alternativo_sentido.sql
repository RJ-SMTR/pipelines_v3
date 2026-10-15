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
            o.data_versao feed_start_date,
            fi.feed_end_date,
            o.tipo_os,
            o.servico,
            o.ativacao,
            o.consorcio,
            o.sentido,
            safe_cast(o.extensao as float64) extensao,
            o.evento,
            o.vista,
        from {{ ref("staging_ordem_servico_trajeto_alternativo_sentido") }} o
        left join {{ ref("feed_info_gtfs") }} fi on o.data_versao = fi.feed_start_date
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
