{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set aux_retorno_negativacao = ref("aux_retorno_negativacao") %}

{% if execute %}
    {% if is_incremental() %}
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
    {% set partitions_query %}
        select distinct concat("'", date(data_autuacao), "'") as partition_date
        from {{ aux_retorno_negativacao }}
        where data = date('{{ var("date_range_end") }}')
    {% endset %}
    {% set partitions = run_query(partitions_query).columns[0].values() %}
{% endif %}

with
    aux_retorno_negativacao as (
        select
            data,
            data_autuacao,
            datetime_retorno,
            produtonome,
            produtoreferencia,
            protocolo,
            contrato,
            cpf,
            resultado
        from {{ aux_retorno_negativacao }}
        where data = date('{{ var("date_range_end") }}')
    ),

    aux_autuacao_negativacao as (
        select
            data,
            contrato,
            data_inclusao,
            data_baixa,
            indicador_nao_inclusao,
            motivo,
            nome,
            cpf,
            cnpj,
            endereco,
            bairro,
            cidade,
            cep,
            estado,
            datavencimento,
            datavenda,
            valor,
            valor_pago
        from {{ ref("aux_autuacao_negativacao") }}
        where
            {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
            {% else %} false
            {% endif %}
    ),

    dados_novos as (
        select
            a.data,
            a.contrato,
            a.data_inclusao,
            a.data_baixa,
            case
                when r.resultado = '00 - INCLUSAO' then date(r.datetime_retorno)
            end as data_confirmacao_inclusao_nova,
            case
                when r.resultado = '01 - BAIXA' then date(r.datetime_retorno)
            end as data_confirmacao_baixa_nova,
            case
                when a.indicador_nao_inclusao is true
                then true
                when r.resultado not in ('00 - INCLUSAO', '01 - BAIXA')
                then true
                else false
            end as indicador_nao_inclusao,
            case
                when a.motivo is not null
                then a.motivo
                when r.resultado not in ('00 - INCLUSAO', '01 - BAIXA')
                then r.resultado
            end as motivo,
            a.nome,
            a.cpf,
            a.cnpj,
            a.endereco,
            a.bairro,
            a.cidade,
            a.cep,
            a.estado,
            a.datavencimento,
            a.datavenda,
            a.valor,
            a.valor_pago
        from aux_autuacao_negativacao a
        left join
            aux_retorno_negativacao r
            on a.contrato = r.contrato
            and a.data = r.data_autuacao
    ),

    sha_dados_novos as (select *, {{ sha_column }} as sha_dado_novo from dados_novos),

    sha_dados_atuais as (
        {% if is_incremental() and partitions | length > 0 %}
            select
                contrato,
                data_confirmacao_inclusao as data_confirmacao_inclusao_atual,
                data_confirmacao_baixa as data_confirmacao_baixa_atual,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}
            where data in ({{ partitions | join(", ") }})
        {% else %}
            select
                cast(null as string) as contrato,
                cast(null as date) as data_confirmacao_inclusao_atual,
                cast(null as date) as data_confirmacao_baixa_atual,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),

    sha_dados_completos as (
        select n.*, a.* except (contrato)
        from sha_dados_novos n
        left join sha_dados_atuais a using (contrato)
    ),

    dados_novos_com_controle as (
        select
            data,
            contrato,
            data_inclusao,
            data_baixa,
            coalesce(
                data_confirmacao_inclusao_nova, data_confirmacao_inclusao_atual
            ) as data_confirmacao_inclusao,
            coalesce(
                data_confirmacao_baixa_nova, data_confirmacao_baixa_atual
            ) as data_confirmacao_baixa,
            indicador_nao_inclusao,
            motivo,
            nome,
            cpf,
            cnpj,
            endereco,
            bairro,
            cidade,
            cep,
            estado,
            datavencimento,
            datavenda,
            valor,
            valor_pago,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then current_datetime("America/Sao_Paulo")
                else datetime_ultima_atualizacao_atual
            end as datetime_ultima_atualizacao,
            '{{ var("version") }}' as versao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then '{{ invocation_id }}'
                else id_execucao_dbt_atual
            end as id_execucao_dbt
        from sha_dados_completos
    ),

    final as (
        select *
        from dados_novos_com_controle
        {% if is_incremental() and partitions | length > 0 %}
            union all
            select *
            from {{ this }}
            where
                data in ({{ partitions | join(", ") }})
                and contrato
                not in (select contrato from dados_novos_com_controle)
        {% endif %}
    )

select *
from final
