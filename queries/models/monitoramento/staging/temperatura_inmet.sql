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
    between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

with
    dados_temperatura as (
        select data_particao as data, horario as hora, id_estacao, temperatura
        from {{ source("clima_estacao_meteorologica", "meteorologia_inmet") }}
        where
            data_particao >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
            {% if is_incremental() %}
                and data_particao {{ incremental_filter }}
            {% endif %}
    ),
    dados_contingencia as (
        select data, hora, id_estacao, temperatura
        from {{ ref("staging_temperatura_inmet") }}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
            {% if is_incremental() %}
                and data_particao
                between date("{{ var('date_range_start') }}") and date_add(
                    date('{{ var("date_range_end") }}'), interval 1 day
                )
                and data {{ incremental_filter }}
            {% endif %}
    ),
    dados_combinados as (
        select *, 0 as priority
        from dados_temperatura

        union all

        select *, 1 as priority
        from dados_contingencia
    ),
    dados_finais as (
        select data, hora, id_estacao, temperatura
        from dados_combinados
        qualify
            row_number() over (partition by data, hora, id_estacao order by priority)
            = 1
    )
select
    data,
    hora,
    id_estacao,
    temperatura,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from dados_finais
