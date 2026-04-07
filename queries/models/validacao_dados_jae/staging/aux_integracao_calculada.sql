{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set aux_particao_calculo_integracao = ref("aux_particao_calculo_integracao") %}

{% if execute and is_incremental() %}

    {% set partitions_query %}
        select particao from {{ aux_particao_calculo_integracao }} where tipo_particao = 'modificada'

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% set adjacent_partitions_query %}
        select particao from {{ aux_particao_calculo_integracao }} where tipo_particao = 'adjacente'

    {% endset %}

    {% set adjacent_partitions = (
        run_query(adjacent_partitions_query).columns[0].values()
    ) %}

{% endif %}

with
    aux_calculo_integracao as (
        select
            data,
            id_integracao,
            case
                when safe_cast(datetime_inicio_integracao as datetime) is null
                then parse_datetime("%Y-%m-%dT%H:%M", datetime_inicio_integracao)
                else datetime(datetime_inicio_integracao)
            end as datetime_inicio_integracao,
            sequencia_integracao,
            tipo_integracao,
            id_transacao,
            case
                when safe_cast(datetime_transacao as datetime) is null
                then parse_datetime("%Y-%m-%dT%H:%M", datetime_transacao)
                else datetime(datetime_transacao)
            end as datetime_transacao,
            case
                when safe_cast(datetime_processamento as datetime) is null
                then parse_datetime("%Y-%m-%dT%H:%M", datetime_processamento)
                else datetime(datetime_processamento)
            end as datetime_processamento,
            modo,
            id_consorcio,
            consorcio,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            sentido,
            id_veiculo,
            id_validador,
            id_cliente,
            hash_cartao,
            cadastro_cliente,
            produto,
            produto_jae,
            tipo_transacao_jae,
            tipo_transacao,
            tipo_usuario,
            meio_pagamento,
            meio_pagamento_jae,
            valor_transacao
        from {{ ref("aux_calculo_integracao") }}
        qualify max(sequencia_integracao) over (partition by id_integracao) > 1
    ),
    integracao_filtrada as (
        select *
        from aux_calculo_integracao
        {% if is_incremental() %}
            {% if partitions | length > 0 %}
                qualify
                    date(max(datetime_transacao) over (partition by id_integracao))
                    in ({{ partitions | join(", ") }})
                    or date(min(datetime_transacao) over (partition by id_integracao))
                    in ({{ partitions | join(", ") }})
            {% else %} where false
            {% endif %}
        {% endif %}
    ),
    particao_completa as (
        select *
        from integracao_filtrada

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select o.*
            from {{ this }} o
            left join integracao_filtrada n using (id_transacao)
            where
                (
                    o.data in ({{ partitions | join(", ") }})
                    or o.data in ({{ adjacent_partitions | join(", ") }})
                )
                and o.id_integracao
                not in (select id_integracao from integracao_filtrada)
            qualify
                not max(n.id_transacao is not null) over (partition by o.id_integracao)

        {% endif %}
    )
select *
from particao_completa
