{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    logs as (
        select
            date(timestamp, 'America/Sao_Paulo') as data,
            datetime(timestamp, 'America/Sao_Paulo') as datetime_evento_log,
            resource.labels.project_id as projeto,
            protopayload_auditlog.authenticationinfo.principalemail as usuario,
            protopayload_auditlog.methodname as metodo,
            protopayload_auditlog.resourcename as id_job,
            datetime(
                safe_cast(
                    protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.starttime
                    as timestamp
                ),
                'America/Sao_Paulo'
            ) as datetime_inicio_job,
            datetime(
                safe_cast(
                    protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.endtime
                    as timestamp
                ),
                'America/Sao_Paulo'
            ) as datetime_fim_job,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.labels
            as labels,
            regexp_extract(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query,
                r'"flow_name"\s*:\s*"([^"]+)"'
            ) as nome_flow,
            regexp_extract(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query,
                r'"dashboard_code"\s*:\s*"([^"]+)"'
            ) as nome_dashboard,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query
            as query,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalprocessedbytes
            as bytes_processados,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
            as bytes_faturados,
            (
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
                / pow(1024, 4)
            ) as tib_processados
        from
            {{
                source(
                    "infraestrutura_staging", "cloudaudit_googleapis_com_data_access"
                )
            }}
        where
            {% if is_incremental() %}
                date(timestamp, 'America/Sao_Paulo') between date_sub(
                    date('{{ var("date_range_start") }}'), interval 1 day
                ) and date_add(date('{{ var("date_range_end") }}'), interval 1 day)
                and date(
                    timestamp,
                    'America/Sao_Paulo'
                ) between date('{{ var("date_range_start") }}') and date(
                    '{{ var("date_range_end") }}'
                )
                and
            {% endif %}
            date(timestamp, 'America/Sao_Paulo')
            >= date('{{ var("data_inicial_logs_bigquery") }}')
            and protopayload_auditlog.methodname = 'jobservice.jobcompleted'
            and coalesce(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.statementtype,
                ''
            )
            != 'SCRIPT'
    ),
    logs_deduplicados as (
        select *
        from logs
        qualify
            row_number() over (
                partition by id_job
                order by
                    datetime_fim_job desc nulls last,
                    datetime_inicio_job desc nulls last,
                    datetime_evento_log desc nulls last
            )
            = 1
    ),
    label_dbt as (
        select data, projeto, id_job, label.value as id_execucao_dbt
        from logs_deduplicados, unnest(labels) as label
        where label.key = 'dbt_invocation_id'
    )
select
    data,
    l.projeto,
    l.usuario,
    l.metodo,
    l.id_job,
    l.datetime_inicio_job,
    l.datetime_fim_job,
    d.id_execucao_dbt,
    l.query,
    coalesce(l.nome_flow, l.nome_dashboard) as processo_execucao,
    case
        when l.nome_flow is not null
        then 'Flow'
        when l.nome_dashboard is not null
        then 'Dashboard'
        else 'Outro'
    end as tipo_processo_execucao,
    l.bytes_processados,
    l.bytes_faturados,
    l.tib_processados,
    l.tib_processados * p.valor_tib_real as custo_real,
    p.valor_tib_real,
    p.valor_tib_dolar,
    p.taxa_conversao_real,
    p.origem as origem_valor,
    '{{ var("version") }}' as versao,
    current_datetime('America/Sao_Paulo') as datetime_ultima_atualizacao
from logs_deduplicados l
left join {{ ref("aux_preco_bigquery") }} p using (data)
left join label_dbt d using (data, projeto, id_job)
