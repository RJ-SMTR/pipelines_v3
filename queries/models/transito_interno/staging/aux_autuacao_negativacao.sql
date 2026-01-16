{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
        },
        cluster_by=["contrato"]
    )
}}

{% set autuacao_controle_negativacao = ref("autuacao_controle_negativacao") %}

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
    {% set partitions_query %}
        select distinct concat("'", date(data_autuacao), "'") as partition_date
        from {{ autuacao_controle_negativacao }}
        where data = date('{{ var("date_range_end") }}')
    {% endset %}
    {% set partitions = run_query(partitions_query).columns[0].values() %}
{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    autuacoes_inclusao as (
        select id_auto_infracao
        from {{ autuacao_controle_negativacao }}
        where data = date('{{ var("date_range_end") }}')
    ),

    dados_novos as (
        select
            data,
            coalesce(nome_proprietario, nome_possuidor_veiculo) as nome,
            coalesce(documento_proprietario, documento_possuidor_veiculo) as cpf,
            endereco_possuidor_veiculo as endereco,
            bairro_possuidor_veiculo as bairro,
            municipio_possuidor_veiculo as cidade,
            cep_possuidor_veiculo as cep,
            case
                upper(uf_possuidor_veiculo)
                when 'ACRE'
                then 'AC'
                when 'ALAGOAS'
                then 'AL'
                when 'AMAPÁ'
                then 'AP'
                when 'AMAPA'
                then 'AP'
                when 'AMAZONAS'
                then 'AM'
                when 'BAHIA'
                then 'BA'
                when 'CEARÁ'
                then 'CE'
                when 'CEARA'
                then 'CE'
                when 'DISTRITO FEDERAL'
                then 'DF'
                when 'ESPÍRITO SANTO'
                then 'ES'
                when 'ESPIRITO SANTO'
                then 'ES'
                when 'GOIÁS'
                then 'GO'
                when 'GOIAS'
                then 'GO'
                when 'MARANHÃO'
                then 'MA'
                when 'MARANHAO'
                then 'MA'
                when 'MATO GROSSO'
                then 'MT'
                when 'MATO GROSSO DO SUL'
                then 'MS'
                when 'MINAS GERAIS'
                then 'MG'
                when 'PARÁ'
                then 'PA'
                when 'PARA'
                then 'PA'
                when 'PARAÍBA'
                then 'PB'
                when 'PARAIBA'
                then 'PB'
                when 'PARANÁ'
                then 'PR'
                when 'PARANA'
                then 'PR'
                when 'PERNAMBUCO'
                then 'PE'
                when 'PIAUÍ'
                then 'PI'
                when 'PIAUI'
                then 'PI'
                when 'RIO DE JANEIRO'
                then 'RJ'
                when 'RIO GRANDE DO NORTE'
                then 'RN'
                when 'RIO GRANDE DO SUL'
                then 'RS'
                when 'RONDÔNIA'
                then 'RO'
                when 'RONDONIA'
                then 'RO'
                when 'RORAIMA'
                then 'RR'
                when 'SANTA CATARINA'
                then 'SC'
                when 'SÃO PAULO'
                then 'SP'
                when 'SAO PAULO'
                then 'SP'
                when 'SERGIPE'
                then 'SE'
                when 'TOCANTINS'
                then 'TO'
                else ''
            end as estado,
            id_auto_infracao as contrato,
            safe_cast(
                format_date('%d%m%Y', data_limite_recurso) as string
            ) as datavencimento,
            safe_cast(
                format_date('%d%m%Y', data_limite_recurso) as string
            ) as datavenda,
            replace(safe_cast(valor_infracao as string), '.', '') as valor,
            valor_pagamento,
            data_pagamento
        from {{ ref("autuacao") }}
        where
            {% if partitions | length > 0 %}
                data in ({{ partitions | join(", ") }})
                and status_infracao = "NP Gerada"
                and descricao_situacao_autuacao in ("Ativo", "Desvinculado")
                and (
                    (
                        data_pagamento is null
                        and id_auto_infracao
                        in (select id_auto_infracao from autuacoes_inclusao)
                    )
                    or (
                        data_pagamento is not null
                        {% if is_incremental() %}
                            and id_auto_infracao in (select contrato from {{ this }})
                        {% else %} and false
                        {% endif %}
                    )
                )
                and (
                    (
                        documento_proprietario is not null
                        and documento_proprietario = documento_possuidor_veiculo
                    )
                    or (
                        documento_proprietario is null
                        and documento_possuidor_veiculo is not null
                    )
                )
                and recurso_penalidade_multa is null
                and processo_defesa_autuacao is null
            {% else %} false
            {% endif %}
    ),

    sha_dados_novos as (select *, {{ sha_column }} as sha_dado_novo from dados_novos),

    sha_dados_atuais as (
        {% if is_incremental() and partitions | length > 0 %}
            select
                contrato,
                data_inclusao as data_inclusao_atual,
                data_baixa as data_baixa_atual,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}
            where data in ({{ partitions | join(", ") }})
        {% else %}
            select
                cast(null as string) as contrato,
                cast(null as date) as data_inclusao_atual,
                cast(null as date) as data_baixa_atual,
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
            coalesce(
                data_inclusao_atual, current_date("America/Sao_Paulo")
            ) as data_inclusao,
            case
                when data_pagamento is not null
                then coalesce(data_baixa_atual, current_date("America/Sao_Paulo"))
            end as data_baixa,
            nome,
            cpf,
            endereco,
            bairro,
            cidade,
            cep,
            estado,
            contrato,
            datavencimento,
            datavenda,
            valor,
            valor_pagamento,
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
                and contrato not in (select contrato from dados_novos_com_controle)
        {% endif %}
    )

select *
from final
