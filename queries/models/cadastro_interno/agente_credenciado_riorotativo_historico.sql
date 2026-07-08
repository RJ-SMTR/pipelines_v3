{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_agente_credenciado_historico",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

-- depends_on: {{ ref('cliente_cpf_jae') }}
{% if execute %}
    {% set staging_partitions_query %}
        select distinct cast(cnpj as int64) as cnpj, cast(documento as int64) as documento
        from {{ ref("staging_agente_credenciado_riorotativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    {% endset %}
    {% set staging_partitions = run_query(staging_partitions_query) %}
    {% set cnpj_partitions = staging_partitions.columns[0].values() | unique | list %}
    {% set cpf_partitions = staging_partitions.columns[1].values() | unique | list %}

    {% set id_cliente_partitions_query %}
        select distinct cast(id_cliente as int64)
        from {{ ref("cliente_cpf_jae") }}
        where cpf_particao in ({{ cpf_partitions | join(", ") if cpf_partitions else "null" }})
    {% endset %}
    {% set id_cliente_partitions = (
        run_query(id_cliente_partitions_query).columns[0].values()
    ) %}
{% endif %}

with
    credenciados as (
        select data, documento, tipo_documento, cnpj
        from {{ ref("staging_agente_credenciado_riorotativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    bloqueios as (
        /*
        considera apenas bloqueios vigentes na data da captura: bloqueio
        expirado que permanece na lista da fonte não marca o agente como
        bloqueado
        */
        select
            data, documento, "CPF" as tipo_documento, motivo_bloqueio, decisao_bloqueio
        from {{ ref("staging_lista_bloqueio_riorotativo") }}
        where
            (data >= data_inicio_bloqueio or data_inicio_bloqueio is null)
            and (data <= data_fim_bloqueio or data_fim_bloqueio is null)
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    ),
    status as (
        select
            coalesce(c.data, b.data) as data,
            c.cnpj,
            coalesce(c.documento, b.documento) as documento,
            coalesce(c.tipo_documento, b.tipo_documento) as tipo_documento,
            if(b.documento is not null, "bloqueado", "ativo") as status,
            b.motivo_bloqueio,
            b.decisao_bloqueio
        from credenciados as c
        full outer join bloqueios as b on c.data = b.data and c.documento = b.documento
        qualify
            row_number() over (
                partition by
                    coalesce(c.data, b.data), c.cnpj, coalesce(c.documento, b.documento)
                order by
                    case when b.documento is not null then 0 else 1 end,
                    b.decisao_bloqueio
            )
            = 1
    ),
    pessoa_juridica as (
        select cnpj, razao_social, nome_fantasia
        from {{ source("rmi_dados_mestres", "pessoa_juridica") }}
        where
            cnpj_particao
            in ({{ cnpj_partitions | join(", ") if cnpj_partitions else "null" }})
    ),
    cliente as (
        select documento, id_cliente
        from {{ ref("cliente_jae") }}
        where
            id_cliente_particao in (
                {{
                    (
                        id_cliente_partitions | join(", ")
                        if id_cliente_partitions
                        else "null"
                    )
                }}
            )
            and tipo_documento = 'CPF'
    ),
    dados_novos as (
        select
            s.data,
            s.cnpj,
            s.documento,
            s.tipo_documento,
            c.id_cliente,
            pj.razao_social,
            pj.nome_fantasia,
            s.status,
            s.motivo_bloqueio,
            s.decisao_bloqueio
        from status as s
        left join cliente as c using (documento)
        left join pessoa_juridica as pj using (cnpj)
    ),
    dados_novos_chave as (
        select
            to_hex(
                sha256(
                    concat(
                        cast(data as string),
                        '-',
                        ifnull(cnpj, 'n/a'),
                        '-',
                        ifnull(documento, 'n/a')
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
        from dados_novos_chave as n
        left join dados_atuais as a using (id_agente_credenciado_historico)
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
