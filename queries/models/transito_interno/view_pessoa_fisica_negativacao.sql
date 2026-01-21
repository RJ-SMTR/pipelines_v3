{{ config(materialized="view") }}
-- depends_on: {{ ref('autuacao_controle_negativacao') }}

{% if execute %}
    {% set partitions_query %}
        select distinct concat("'", date(data_autuacao), "'") as partition_date
        from {{ ref("autuacao_controle_negativacao") }}
        where data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
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
