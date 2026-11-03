{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    eventos as (
        select *
        from {{ ref("aux_logs_bigquery") }}
        {% if is_incremental() %}
            where
                data between date_sub(
                    date('{{ var("date_range_start") }}'), interval 1 day
                ) and date_add(date('{{ var("date_range_end") }}'), interval 1 day)
        {% endif %}
    ),
    parentesco_jobs as (
        select id_job_metadados as id_job, id_job_pai
        from eventos
        where id_job_metadados is not null and id_job_pai is not null
        qualify
            row_number() over (
                partition by id_job_metadados
                order by timestamp_evento_log desc, id_evento_log desc
            )
            = 1
    ),
    logs_deduplicados as (
        select
            *,
            regexp_extract(query, r'"flow_name"\s*:\s*"([^"]+)"') as nome_flow,
            regexp_extract(query, r'"dashboard_code"\s*:\s*"([^"]+)"') as nome_dashboard
        from eventos
        where metodo = 'jobservice.jobcompleted' and id_job is not null
        qualify
            row_number() over (
                partition by id_job
                order by
                    datetime_fim_job desc nulls last,
                    datetime_inicio_job desc nulls last,
                    timestamp_evento_log desc nulls last,
                    id_evento_log desc
            )
            = 1
    ),
    label_dbt as (
        select id_job, label.value as id_execucao_dbt
        from logs_deduplicados, unnest(labels) as label
        where label.key = 'dbt_invocation_id'
    ),
    logs_atribuidos as (
        select
            l.*,
            parentesco.id_job_pai as id_job_pai_resolvido,
            coalesce(d.id_execucao_dbt, dp.id_execucao_dbt) as id_execucao_dbt,
            coalesce(
                l.nome_flow, l.nome_dashboard, pai.nome_flow, pai.nome_dashboard
            ) as processo_execucao,
            case
                when l.nome_flow is not null
                then 'Flow'
                when l.nome_dashboard is not null
                then 'Dashboard'
                when pai.nome_flow is not null
                then 'Flow'
                when pai.nome_dashboard is not null
                then 'Dashboard'
                else 'Outro'
            end as tipo_processo_execucao
        from logs_deduplicados l
        left join parentesco_jobs parentesco using (id_job)
        left join logs_deduplicados pai on parentesco.id_job_pai = pai.id_job
        left join label_dbt d on l.id_job = d.id_job
        left join label_dbt dp on pai.id_job = dp.id_job
    )
select
    l.data,
    l.datetime_inicio_job,
    l.datetime_fim_job,
    l.projeto,
    l.usuario,
    l.metodo,
    l.id_job,
    l.id_job_pai_resolvido as id_job_pai,
    l.id_execucao_dbt,
    l.tipo_statement,
    l.status_job,
    l.codigo_erro_job,
    l.mensagem_erro_job,
    l.tabela_destino,
    l.tabelas_referenciadas,
    if(
        array_length(l.labels) > 0,
        json_object(
            array(select label.key from unnest(l.labels) as label),
            array(select label.value from unnest(l.labels) as label)
        ),
        null
    ) as labels,
    l.query,
    l.processo_execucao,
    l.tipo_processo_execucao,
    l.bytes_processados,
    l.bytes_faturados,
    l.bytes_faturados / pow(1024, 4) as tib_processados,
    l.bytes_faturados / pow(1024, 4) * p.valor_tib_real as custo_real,
    p.valor_tib_real,
    p.valor_tib_dolar,
    p.taxa_conversao_real,
    p.origem as origem_valor,
    '{{ var("version") }}' as versao,
    current_datetime('America/Sao_Paulo') as datetime_ultima_atualizacao
from logs_atribuidos l
left join {{ ref("aux_preco_bigquery") }} p on l.data = p.data
where
    coalesce(l.tipo_statement, '') != 'SCRIPT'
    {% if is_incremental() %}
        and l.data between date('{{ var("date_range_start") }}') and date(
            '{{ var("date_range_end") }}'
        )
    {% endif %}
