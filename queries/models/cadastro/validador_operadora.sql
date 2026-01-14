{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_device_operadora",
    )
}}

with
    staging_device_operadora as (
        select
            id as id_device_operadora,
            cd_operadora_transporte as id_operadora_jae,
            data_inclusao as datetime_inicio_validade,
            data_desassociacao as datetime_fim_validade,
            nr_serial as id_validador
        from {{ ref("staging_device_operadora") }}
        where
            id_tipo_device = '1'  -- validador
            {% if is_incremental() %}
                and date(data) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
            {% endif %}
    )
select
    s.id_device_operadora,
    o.modo,
    id_operadora_jae,
    o.operadora,
    o.operadora_completo,
    o.tipo_documento,
    o.documento,
    s.datetime_inicio_validade,
    s.datetime_fim_validade,
    s.id_validador,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from staging_device_operadora s
join {{ ref("operadoras") }} o using (id_operadora_jae)
