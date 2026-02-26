{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

/*
consulta as partições a serem atualizadas com base nas transações capturadas entre date_range_start e date_range_end
e as integrações capturadas entre date_range_start e date_range_end
*/
{% set transacao_table = ref("transacao") %}
{% set transacao_riocard_table = ref("transacao_riocard") %}
{% if execute %}
    {% if is_incremental() %}
        -- Transações Jaé
        {% set partitions_query %}
      select
        concat("'", parse_date("%Y%m%d", partition_id), "'") as data
      from
        -- `{{ transacao_table.database }}.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        `rj-smtr.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
      where
        table_name = "{{ transacao_table.identifier }}"
        and partition_id != "__NULL__"
        and datetime(last_modified_time, "America/Sao_Paulo") between datetime("{{var('date_range_start')}}") and (datetime("{{var('date_range_end')}}"))

      union distinct

      select
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") as data
      from
        -- `{{ transacao_riocard_table.database }}.{{ transacao_riocard_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        `rj-smtr.{{ transacao_riocard_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
      where
        table_name = "{{ transacao_riocard_table.identifier }}"
        and partition_id != "__NULL__"
        and datetime(last_modified_time, "America/Sao_Paulo") between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")

        {% endset %}

        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
    {% endif %}
{% endif %}

select
    * except (id_transacao, geo_point_transacao, valor_pagamento),
    count(id_transacao) as quantidade_passageiros,
    sum(valor_pagamento) as valor_total_transacao,
    '{{ var("version") }}' as versao
from {{ ref("aux_passageiro_hora") }}
where
    {% if is_incremental() %}
        {% if partition_list | length > 0 %} data in ({{ partition_list | join(", ") }})
        {% else %} data = "2000-01-01"
        {% endif %}
    {% else %} data >= "2023-07-19"
    {% endif %}
group by all
