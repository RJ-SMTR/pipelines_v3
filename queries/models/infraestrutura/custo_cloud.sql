{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
  date(_PARTITIONTIME, "America/Sao_Paulo") between
    date_sub(date("{{var('date_range_start')}}"), interval 1 day) and
    date_add(date("{{var('date_range_end')}}"), interval 1 day)
{% endset %}

{% set billing_staging = source(
    "infraestrutura_staging",
    "gcp_billing_export_resource_v1_010693_B1EC8D_C0D4DF",
) %}

{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            select distinct concat("'", date(usage_start_time, "America/Sao_Paulo"), "'") as data
            from {{ billing_staging }}
            where {{ incremental_filter }}
              and date(usage_start_time, "America/Sao_Paulo") between
                date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
        {% endset %}
        {% set partitions = run_query(partitions_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    billing as (
        select *, date(_partitiontime, "America/Sao_Paulo") as data_particao_fonte
        from {{ billing_staging }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    novos_dados as (
        select
            date(usage_start_time, "America/Sao_Paulo") as data,
            service.description as tipo_servico,
            sku.description as sku,
            project.name as projeto,
            cost as custo,
            currency_conversion_rate as taxa_conversao_real,
            usage.amount as quantidade_uso,
            usage.unit as unidade_uso,
            usage.amount_in_pricing_units as quantidade_unidade_preco,
            usage.pricing_unit as unidade_preco,
            data_particao_fonte,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from billing
        where
            usage_start_time >= '{{ var("data_inicial_custo_cloud") }}'
            and date(
                usage_start_time,
                "America/Sao_Paulo"
            ) between date("{{var('date_range_start')}}") and date(
                "{{var('date_range_end')}}"
            )
    )
select *
from novos_dados
{% if is_incremental() and partitions | length > 0 %}
    union all
    select *
    from {{ this }}
    where
        data in ({{ partitions | join(", ") }})
        and data_particao_fonte
        not in (select distinct data_particao_fonte from novos_dados)
{% endif %}
