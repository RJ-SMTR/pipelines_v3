{{ config(materialized="ephemeral") }}

/*
    Eventos do log de auditoria do BigQuery. Até a véspera de
    data_inicio_sink_unificada_logs_bigquery os logs vêm dos datasets bq_logs de cada
    projeto. A partir dessa data vêm da sink unificada infraestrutura_staging, que
    reúne todos os projetos mas só começou a receber dados em 2026-04-01 às 13:06 UTC.
*/
{% set data_inicio_sink = var("data_inicio_sink_unificada_logs_bigquery") %}
{% set fontes_logs = [
    {
        "relation": source(
            "bq_logs_prod", "cloudaudit_googleapis_com_data_access"
        ),
        "filtro_data": "< date('" ~ data_inicio_sink ~ "')",
    },
    {
        "relation": source(
            "bq_logs_dev", "cloudaudit_googleapis_com_data_access"
        ),
        "filtro_data": "< date('" ~ data_inicio_sink ~ "')",
    },
    {
        "relation": source(
            "bq_logs_staging", "cloudaudit_googleapis_com_data_access"
        ),
        "filtro_data": "< date('" ~ data_inicio_sink ~ "')",
    },
    {
        "relation": source(
            "infraestrutura_staging", "cloudaudit_googleapis_com_data_access"
        ),
        "filtro_data": ">= date('" ~ data_inicio_sink ~ "')",
    },
] %}

{% for fonte in fontes_logs %}
    select
        date(timestamp, 'America/Sao_Paulo') as data,
        timestamp as timestamp_evento_log,
        insertid as id_evento_log,
        resource.labels.project_id as projeto,
        protopayload_auditlog.authenticationinfo.principalemail as usuario,
        protopayload_auditlog.methodname as metodo,
        protopayload_auditlog.resourcename as id_job,
        protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.labels
        as labels,
        protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query
        as query,
        protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.statementtype
        as tipo_statement,
        protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatus.state
        as status_job,
        protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatus.error.code
        as codigo_erro_job,
        protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatus.error.message
        as mensagem_erro_job,
        if(
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.destinationtable.tableid
            is not null,
            concat(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.destinationtable.projectid,
                '.',
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.destinationtable.datasetid,
                '.',
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.destinationtable.tableid
            ),
            null
        ) as tabela_destino,
        array(
            select concat(t.projectid, '.', t.datasetid, '.', t.tableid)
            from
                unnest(
                    protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.referencedtables
                ) as t
        ) as tabelas_referenciadas,
        datetime(
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.starttime,
            'America/Sao_Paulo'
        ) as datetime_inicio_job,
        datetime(
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.endtime,
            'America/Sao_Paulo'
        ) as datetime_fim_job,
        protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalprocessedbytes
        as bytes_processados,
        protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
        as bytes_faturados,
        coalesce(
            json_value(protopayload_auditlog.metadatajson, '$.jobChange.job.jobName'),
            json_value(protopayload_auditlog.metadatajson, '$.jobInsertion.job.jobName')
        ) as id_job_metadados,
        coalesce(
            json_value(
                protopayload_auditlog.metadatajson,
                '$.jobChange.job.jobStats.parentJobName'
            ),
            json_value(
                protopayload_auditlog.metadatajson,
                '$.jobInsertion.job.jobStats.parentJobName'
            )
        ) as id_job_pai
    from {{ fonte.relation }}
    where
        date(timestamp, 'America/Sao_Paulo')
        >= date('{{ var("data_inicial_logs_bigquery") }}')
        and date(timestamp, 'America/Sao_Paulo') {{ fonte.filtro_data }}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
