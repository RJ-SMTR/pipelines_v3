{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        full_refresh=false,
    )
}}

{% set incremental_filter %}
    between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

with
    dados as (
        select
            data_particao as data,
            extract(time from data_medicao) as hora,
            data_medicao,
            id_estacao,
            temperatura,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            "{{ var('version') }}" as versao,
            '{{ invocation_id }}' as id_execucao_dbt
        from {{ source("clima_estacao_meteorologia", "meteorologia_alertario") }}
        where
            data_particao >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
            and data_medicao is not null
            {% if is_incremental() %}
                and data_particao {{ incremental_filter }}
                /* Preserva as partições já materializadas na cadência histórica, pois as fontes não têm o histórico completo */
                and data_particao not in (
                    select data
                    from {{ this }}
                    where
                        data {{ incremental_filter }}
                        and data
                        < date("{{ var('DATA_ALERTARIO_CADENCIA_5_MINUTOS') }}")
                )
            {% endif %}
    ),
    dados_deduplicados as (
        select * except (data_medicao)
        from dados
        qualify
            row_number() over (
                partition by data, hora, id_estacao order by data_medicao desc
            )
            = 1
    )
select *
from dados_deduplicados
