{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    prod as (
        select
            date(timestamp, 'America/Sao_Paulo') as data,
            resource.labels.project_id as projeto,
            protopayload_auditlog.authenticationinfo.principalemail as usuario,
            protopayload_auditlog.methodname as metodo,
            protopayload_auditlog.resourcename as id_job,
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

        from {{ source("bq_logs_prod", "cloudaudit_googleapis_com_data_access_*") }}
        where
            {% if is_incremental() %}
                parse_date('%Y%m%d', _table_suffix) between date_sub(
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
            parse_date('%Y%m%d', _table_suffix)
            >= date('{{ var("data_inicial_logs_bigquery") }}')
            and date(timestamp, 'America/Sao_Paulo')
            >= date('{{ var("data_inicial_logs_bigquery") }}')
    ),
    dev as (
        select
            date(timestamp, 'America/Sao_Paulo') as data,
            resource.labels.project_id as projeto,
            protopayload_auditlog.authenticationinfo.principalemail as usuario,
            protopayload_auditlog.methodname as metodo,
            protopayload_auditlog.resourcename as id_job,
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

        from {{ source("bq_logs_dev", "cloudaudit_googleapis_com_data_access") }}
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
    ),
    staging as (
        select
            date(timestamp, 'America/Sao_Paulo') as data,
            resource.labels.project_id as projeto,
            protopayload_auditlog.authenticationinfo.principalemail as usuario,
            protopayload_auditlog.methodname as metodo,
            protopayload_auditlog.resourcename as id_job,
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

        from {{ source("bq_logs_staging", "cloudaudit_googleapis_com_data_access") }}
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
    ),
    union_projetos as (
        select *
        from prod

        union all

        select *
        from dev

        union all

        select *
        from staging
    ),
    label_dbt as (
        select data, projeto, id_job, label.value as id_execucao_dbt
        from union_projetos, unnest(labels) as label
        where label.key = 'dbt_invocation_id'
    )
select
    data,
    u.projeto,
    u.usuario,
    u.metodo,
    u.id_job,
    d.id_execucao_dbt,
    u.query,
    coalesce(u.nome_flow, u.nome_dashboard) as processo_execucao,
    case
        when u.nome_flow is not null
        then 'Flow'
        when u.nome_dashboard is not null
        then 'Dashboard'
        else 'Outro'
    end as tipo_processo_execucao,
    u.bytes_processados,
    u.bytes_faturados,
    u.tib_processados,
    u.tib_processados * p.valor_tib_real as custo_real,
    p.valor_tib_real,
    p.valor_tib_dolar,
    p.taxa_conversao_real,
    p.origem as origem_valor,
    '{{ var("version") }}' as versao,
    current_datetime('America/Sao_Paulo') as datetime_ultima_atualizacao
from union_projetos u
left join {{ ref("aux_preco_bigquery") }} p using (data)
left join label_dbt d using (data, projeto, id_job)
where usuario is not null and bytes_faturados > 0
