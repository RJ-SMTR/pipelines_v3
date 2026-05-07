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
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% set incremental_filter_inmet %}
    data between date("{{var('date_range_start')}}") and date_add(
        date("{{var('date_range_end')}}"), interval 1 day
    )
{% endset %}

with
    inmet_ajustado as (
        select
            case
                when data >= date("{{ var('DATA_SUBSIDIO_V23_INICIO') }}")
                then
                    extract(
                        date from datetime_sub(datetime(data, hora), interval 1 hour)
                    )
                else data
            end as data,
            case
                when data >= date("{{ var('DATA_SUBSIDIO_V23_INICIO') }}")
                then
                    extract(
                        hour from datetime_sub(datetime(data, hora), interval 1 hour)
                    )
                else extract(hour from hora)
            end as hora,
            temperatura
        from {{ ref("temperatura_inmet") }}
        where
            {{ incremental_filter_inmet }}
            and id_estacao in ("A621", "A652", "A636", "A602")  -- Estações do Rio de Janeiro
    ),
    inmet as (
        select data, hora, max(temperatura) as temperatura, "inmet" as fonte
        from inmet_ajustado
        where {{ incremental_filter }}
        group by 1, 2
    ),
    alertario as (
        select
            data,
            extract(hour from hora) as hora,
            max(temperatura) as temperatura,
            "alertario" as fonte
        from {{ ref("temperatura_alertario") }}
        where {{ incremental_filter }}
        group by 1, 2
    )
select
    coalesce(i.data, a.data) as data,
    coalesce(i.hora, a.hora) as hora,
    coalesce(i.temperatura, a.temperatura) as temperatura,
    case when i.temperatura is not null then i.fonte else a.fonte end as fonte,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from inmet i
full outer join alertario a using (data, hora)
