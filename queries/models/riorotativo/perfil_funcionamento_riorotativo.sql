{{
    config(
        materialized="table",
        alias="perfil_funcionamento",
    )
}}

/* backfill com janela antiga desliga o modelo: ver macro is_current_state_enabled */
{% if execute %}
    {% set last_partition_query %}
        select max(data)
        from {{ ref("staging_perfil_funcionamento_riorotativo") }}
        where data between date_sub(date("{{ var('date_range_start') }}"), interval 1 day) and date("{{ var('date_range_end') }}")
    {% endset %}
    {% set last_partition = run_query(last_partition_query).columns[0].values()[0] %}
{% endif %}

{% if is_current_state_enabled() %}
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

{% else %} select * from {{ this }}
{% endif %}
