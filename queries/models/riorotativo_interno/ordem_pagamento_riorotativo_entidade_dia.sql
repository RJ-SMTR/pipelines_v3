{{
    config(
        materialized="incremental",
        alias="ordem_pagamento_riorotativo_entidade_dia",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    verificacao_entidade as (
        select v.*, cnpj
        from {{ ref("verificacao_guardador_veiculo") }} as v
        cross join unnest(v.cnpjs_entidade) as cnpj
        where
            v.indicador_verificacao_valida
            and v.data between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
    ),
    entidade as (
        select
            cnpj, data_inicio_vigencia, data_fim_vigencia, razao_social, nome_fantasia
        from {{ ref("entidade_credenciadora_riorotativo_historico") }}
    )
select
    v.data as data_ordem,
    v.cnpj,
    any_value(coalesce(e.nome_fantasia, e.razao_social)) as nome_entidade,
    count(*) as quantidade_verificacao_valida,
    count(distinct v.cpf_guardador_veiculo) as quantidade_guardador_ativo,
    sum(v.valor_repasse_entidade_unitario) as valor_repasse_entidade,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from verificacao_entidade as v
left join
    entidade as e
    on v.cnpj = e.cnpj
    and v.data >= e.data_inicio_vigencia
    and (v.data <= e.data_fim_vigencia or e.data_fim_vigencia is null)
group by data_ordem, v.cnpj
