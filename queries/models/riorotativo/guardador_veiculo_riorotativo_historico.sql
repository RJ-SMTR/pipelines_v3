{{
    config(
        materialized="incremental",
        alias="guardador_veiculo_historico",
        incremental_strategy="merge",
        unique_key="id_guardador_veiculo_historico",
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
        from {{ ref("staging_guardador_veiculo_riorotativo") }}
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
    credenciados as (
        select data, documento, tipo_documento, cnpj
        from {{ ref("staging_guardador_veiculo_riorotativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    bloqueios as (
        {#
            TODO: substituir o stub de nulls pelo código comentado quando a
            captura de lista_bloqueio existir:

            /*
            considera apenas bloqueios vigentes na data da captura: bloqueio
            expirado que permanece na lista da fonte não marca o agente como
            bloqueado; se houver mais de um bloqueio vigente, mantém o
            iniciado mais recentemente
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
        #}
        select
            cast(null as date) as data,
            cast(null as string) as documento,
            cast(null as string) as motivo_bloqueio,
            cast(null as string) as decisao_bloqueio
    ),
    status as (
        /*
        documento bloqueado que não está na lista de credenciados não gera
        linha: o grão é o agente credenciado; a lista bruta de bloqueios fica
        em staging_lista_bloqueio_riorotativo
        */
        select
            c.data,
            c.cnpj,
            c.documento,
            c.tipo_documento,
            if(b.documento is not null, "bloqueado", "ativo") as status,
            b.motivo_bloqueio,
            b.decisao_bloqueio
        from credenciados as c
        left join bloqueios as b using (data, documento)
    ),
    entidade_credenciadora as (
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
            ec.razao_social,
            ec.nome_fantasia,
            s.status,
            s.motivo_bloqueio,
            s.decisao_bloqueio
        from status as s
        left join cliente as c using (documento)
        left join
            entidade_credenciadora as ec
            on s.cnpj = ec.cnpj
            and s.data >= ec.data_inicio_vigencia
            and (s.data <= ec.data_fim_vigencia or ec.data_fim_vigencia is null)
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
            ) as id_guardador_veiculo_historico,
            *
        from dados_novos
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo from dados_novos_chave
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                id_guardador_veiculo_historico,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}
            where {{ incremental_filter }}
        {% else %}
            select
                cast(null as string) as id_guardador_veiculo_historico,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_guardador_veiculo_historico)
        from sha_dados_novos as n
        left join sha_dados_atuais as a using (id_guardador_veiculo_historico)
    ),
    guardador_veiculo_colunas_controle as (
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
from guardador_veiculo_colunas_controle
