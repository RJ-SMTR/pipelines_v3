{{
    config(
        materialized="table",
        alias="perfil_funcionamento",
        enabled=is_current_state_enabled(),
    )
}}

/* backfill com janela antiga desliga o modelo: ver macro is_current_state_enabled */
{% if execute %}
    {% set last_partition_query %}
        select max(data)
        from {{ ref("staging_perfil_funcionamento_riorotativo") }}
        where data between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}")
    {% endset %}
    {% set last_partition = run_query(last_partition_query).columns[0].values()[0] %}
    {% if last_partition is none %}
        {{
            exceptions.raise_compiler_error(
                "No staging_perfil_funcionamento_riorotativo partitions found between date_range_start and date_range_end"
            )
        }}
    {% endif %}
{% endif %}

select
    perfil_funcionamento_codigo as id_perfil_funcionamento,
    perfil_funcionamento_nome as nome,
    perfil_funcionamento_dia_semana as dias_semana,
    perfil_funcionamento_horario_inicio as horario_inicio,
    perfil_funcionamento_horario_fim as horario_fim,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("staging_perfil_funcionamento_riorotativo") }}
where data = date("{{ last_partition }}")
