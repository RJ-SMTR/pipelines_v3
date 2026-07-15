/* TODO: reativar quando a captura da entidade de verificação existir */
{{
    config(
        enabled=false,
        materialized="incremental",
        alias="agente_verificacao_historico",
        incremental_strategy="merge",
        unique_key="id_agente_verificacao_historico",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

-- depends_on: {{ ref('cliente_cpf_jae') }}
{% if execute %}
    {% set staging_partitions_query %}
        select distinct cast(documento as int64)
        from {{ ref("staging_agente_verificacao_riorotativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    {% endset %}
    {% set cpf_partitions = (
        run_query(staging_partitions_query).columns[0].values()
        | unique
        | list
    ) %}

    {% set id_cliente_partitions_query %}
        select distinct cast(id_cliente as int64)
        from {{ ref("cliente_cpf_jae") }}
        where cpf_particao in ({{ cpf_partitions | join(", ") if cpf_partitions else "null" }})
    {% endset %}
    {% set id_cliente_partitions = (
        run_query(id_cliente_partitions_query).columns[0].values()
    ) %}
{% endif %}

{% if execute and is_incremental() %}
    {% set columns = (
        list_columns()
        | reject(
            "in",
            ["versao", "datetime_ultima_atualizacao", "id_execucao_dbt"],
        )
        | list
    ) %}
    {% set sha_column %}
        sha256(
            concat(
                {% for c in columns %}
                    ifnull(cast({{ c }} as string), 'n/a')
                    {% if not loop.last %}, {% endif %}

                {% endfor %}
            )
        )
    {% endset %}
{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    verificacao as (
        select data, documento, tipo_documento, cnpj
        from {{ ref("staging_agente_verificacao_riorotativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    bloqueios as (
        /*
        considera apenas bloqueios vigentes na data da captura: bloqueio
        expirado que permanece na lista da fonte não marca o agente como
        bloqueado; se houver mais de um bloqueio vigente, mantém o iniciado
        mais recentemente
        */
        select data, documento, motivo_bloqueio, decisao_bloqueio
        from {{ ref("staging_lista_bloqueio_riorotativo") }}
        where
            data >= data_inicio_bloqueio
            and (data <= data_fim_bloqueio or data_fim_bloqueio is null)
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by data, documento
                order by
                    data_inicio_bloqueio desc,
                    ultima_atualizacao desc,
                    decisao_bloqueio desc
            )
            = 1
    ),
    status as (
        /*
        documento bloqueado que não está na lista de agentes de verificação
        não gera linha: o grão é o agente de verificação; a lista bruta de
        bloqueios fica em staging_lista_bloqueio_riorotativo
        */
        select
            v.data,
            v.cnpj,
            v.documento,
            v.tipo_documento,
            if(b.documento is not null, "bloqueado", "ativo") as status,
            b.motivo_bloqueio,
            b.decisao_bloqueio
        from verificacao as v
        left join bloqueios as b using (data, documento)
    ),
    entidade as (
        select
            cnpj, data_inicio_vigencia, data_fim_vigencia, razao_social, nome_fantasia
        from {{ ref("entidade_credenciadora_riorotativo_historico") }}
    ),
    cliente as (
        select documento, id_cliente, nome, email, telefone
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
            c.nome,
            c.email,
            c.telefone,
            e.razao_social,
            e.nome_fantasia,
            s.status,
            s.motivo_bloqueio,
            s.decisao_bloqueio
        from status as s
        left join cliente as c using (documento)
        left join
            entidade as e
            on s.cnpj = e.cnpj
            and (s.data >= e.data_inicio_vigencia or e.data_inicio_vigencia is null)
            and (s.data <= e.data_fim_vigencia or e.data_fim_vigencia is null)
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
            ) as id_agente_verificacao_historico,
            *
        from dados_novos
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo from dados_novos_chave
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                id_agente_verificacao_historico,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}
            where {{ incremental_filter }}
        {% else %}
            select
                cast(null as string) as id_agente_verificacao_historico,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_agente_verificacao_historico)
        from sha_dados_novos as n
        left join sha_dados_atuais as a using (id_agente_verificacao_historico)
    ),
    agente_verificacao_colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual
            ),
            '{{ var("version") }}' as versao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then current_datetime("America/Sao_Paulo")
                else datetime_ultima_atualizacao_atual
            end as datetime_ultima_atualizacao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then '{{ invocation_id }}'
                else id_execucao_dbt_atual
            end as id_execucao_dbt
        from sha_dados_completos
    )
select *
from agente_verificacao_colunas_controle
