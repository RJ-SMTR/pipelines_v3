{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_agente_credenciado_historico",
    )
}}

{% set staging_riorotativo_credenciado = ref("staging_riorotativo_credenciado") %}
{% set staging_riorotativo_lista_bloqueio = ref("staging_riorotativo_lista_bloqueio") %}


{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and datetime_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}


with
    {% if is_incremental() %}
        documentos_atualizados as (
            select distinct documento
            from {{ staging_riorotativo_credenciado }}
            where {{ incremental_filter }}
            union distinct
            select distinct documento
            from {{ staging_riorotativo_lista_bloqueio }}
            where {{ incremental_filter }}
        ),
    {% endif %}

    credenciados as (
        select
            cnpj,
            id_cliente,
            documento,
            "ativo" as status,
            cast(null as string) as motivo_bloqueio,
            cast(null as string) as decisao_bloqueio,
            data as data_inicio,
            date(null) as data_fim
        from {{ staging_riorotativo_credenciado }} c
        {% if is_incremental() %}
            where documento in (select documento from documentos_atualizados)
        {% endif %}
    ),
    bloqueios as (
        select
            c.cnpj,
            c.id_cliente,
            b.documento,
            "bloqueado" as status,
            b.motivo_bloqueio,
            b.decisao_bloqueio,
            b.data_inicio_bloqueio as data_inicio,
            b.data_fim_bloqueio as data_fim
        from {{ staging_riorotativo_lista_bloqueio }} b
        left join {{ staging_riorotativo_credenciado }} c using (documento)
        {% if is_incremental() %}
            where b.documento in (select documento from documentos_atualizados)
        {% endif %}
    ),
    dados_novos as (
        select *
        from credenciados
        union all by name
        select *
        from bloqueios
    ),
    dados_novos_chave as (
        select
            to_hex(
                sha256(
                    concat(
                        ifnull(cnpj, 'n/a'),
                        ifnull(id_cliente, 'n/a'),
                        ifnull(status, 'n/a'),
                        ifnull(decisao_bloqueio, 'n/a'),
                        ifnull(cast(data_inicio as string), 'n/a'),
                        ifnull(cast(data_fim as string), 'n/a')
                    )
                )
            ) as id_agente_credenciado_historico,
            *
        from dados_novos
    ),
    dados_atuais as (
        {% if is_incremental() %}
            select
                id_agente_credenciado_historico,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}
        {% else %}
            select
                cast(null as string) as id_agente_credenciado_historico,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    dados_completos as (
        select n.*, a.* except (id_agente_credenciado_historico)
        from dados_novos_chave n
        left join dados_atuais a using (id_agente_credenciado_historico)
    ),
    agente_credenciado_colunas_controle as (
        select
            * except (datetime_ultima_atualizacao_atual, id_execucao_dbt_atual),
            '{{ var("version") }}' as versao,
            coalesce(
                datetime_ultima_atualizacao_atual, current_datetime("America/Sao_Paulo")
            ) as datetime_ultima_atualizacao,
            coalesce(id_execucao_dbt_atual, '{{ invocation_id }}') as id_execucao_dbt
        from dados_completos
    )
select *
from agente_credenciado_colunas_controle
