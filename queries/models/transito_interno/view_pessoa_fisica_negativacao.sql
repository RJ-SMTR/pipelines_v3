{{ config(materialized="view") }}

{% set aux_autuacao_negativacao = ref("aux_autuacao_negativacao") %}
{% if execute %}
    {% set partitions_query %}
        select distinct
            concat(
                "'", format_date('%Y-%m-%d', parse_date('%Y%m%d', partition_id)), "'"
            ) as partition_date
        from `{{ aux_autuacao_negativacao.database }}.{{ aux_autuacao_negativacao.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = "{{ aux_autuacao_negativacao.identifier }}"
            and partition_id != "__NULL__"
            and date(last_modified_time, "America/Sao_Paulo")
                between date("{{ var('date_range_start') }}")
                and date("{{ var('date_range_end') }}")
    {% endset %}
    {% set partitions = run_query(partitions_query).columns[0].values() %}
{% endif %}

select
    data,
    data_inclusao,
    data_baixa,
    nome,
    cpf,
    endereco,
    bairro,
    cidade,
    cep,
    ifnull(estado, '') as estado,
    contrato,
    datavencimento,
    datavenda,
    valor
from {{ ref("aux_autuacao_negativacao") }}
where indicador_nao_inclusao is false and data in ({{ partitions | join(", ") }})
order by data, contrato
