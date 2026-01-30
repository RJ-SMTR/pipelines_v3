{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
        },
    )
}}

{% set autuacao_controle_negativacao = ref("autuacao_controle_negativacao") %}
{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% if execute %}
    {% if is_incremental() %}
        {% set columns = (
            list_columns()
            | reject(
                "in",
                [
                    "data_inclusao",
                    "data_baixa",
                    "indicador_nao_inclusao",
                    "motivo",
                    "versao",
                    "datetime_ultima_atualizacao",
                    "id_execucao_dbt",
                ],
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
        {% set partitions_baixa_query %}
            select distinct concat("'", date(data), "'") as partition_date
            from {{ this }}
            where data_baixa is null
        {% endset %}
        {% set partitions_baixa = (
            run_query(partitions_baixa_query).columns[0].values()
        ) %}
    {% else %}
        {% set sha_column %}
        cast(null as bytes)
        {% endset %}
        {% set partitions_baixa = [] %}
    {% endif %}
    {% set partitions_inclusao_query %}
        select distinct concat("'", date(data_autuacao), "'") as partition_date
        from {{ autuacao_controle_negativacao }}
        where {{ incremental_filter }}
    {% endset %}
    {% set partitions_inclusao = (
        run_query(partitions_inclusao_query).columns[0].values()
    ) %}
    {% set partitions = (
        (partitions_inclusao | list + partitions_baixa | list) | unique | list
    ) %}
{% endif %}

with
    autuacoes_inclusao as (
        select data as data_lote, data_autuacao, id_auto_infracao
        from {{ autuacao_controle_negativacao }}
        where {{ incremental_filter }}
    ),

    dados_novos as (
        select
            a.data,
            ai.data_lote,
            coalesce(a.nome_proprietario, a.nome_possuidor_veiculo) as nome,
            case
                when
                    length(
                        coalesce(
                            a.documento_proprietario, a.documento_possuidor_veiculo
                        )
                    )
                    = 11
                then coalesce(a.documento_proprietario, a.documento_possuidor_veiculo)
            end as cpf,
            case
                when
                    length(
                        coalesce(
                            a.documento_proprietario, a.documento_possuidor_veiculo
                        )
                    )
                    = 14
                then coalesce(a.documento_proprietario, a.documento_possuidor_veiculo)
            end as cnpj,
            a.endereco_possuidor_veiculo as endereco,
            a.bairro_possuidor_veiculo as bairro,
            a.municipio_possuidor_veiculo as cidade,
            a.cep_possuidor_veiculo as cep,
            case
                upper(a.uf_possuidor_veiculo)
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
                else null
            end as estado,
            a.id_auto_infracao as contrato,
            safe_cast(
                format_date('%d%m%Y', a.data_limite_recurso) as string
            ) as datavencimento,
            safe_cast(
                format_date('%d%m%Y', a.data_limite_recurso) as string
            ) as datavenda,
            replace(safe_cast(a.valor_infracao as string), '.', '') as valor,
            a.valor_pago,
            a.data_pagamento
        {# from {{ ref("autuacao") }} as a #}
        from `rj-smtr.transito.autuacao` as a
        left join
            autuacoes_inclusao as ai
            on ai.id_auto_infracao = a.id_auto_infracao
            and ai.data_autuacao = a.data
        where
            {% if partitions | length > 0 %}
                a.data in ({{ partitions | join(", ") }})
                and (
                    (
                        a.status_infracao = "NP Gerada"
                        and a.descricao_situacao_autuacao in ("Ativo", "Desvinculado")
                        and a.data_pagamento is null
                        and (
                            (
                                a.documento_proprietario is not null
                                and a.documento_proprietario
                                = a.documento_possuidor_veiculo
                            )
                            or (
                                a.documento_proprietario is null
                                and a.documento_possuidor_veiculo is not null
                            )
                        )
                        and a.recurso_penalidade_multa is null
                        and a.processo_defesa_autuacao is null
                        and a.id_auto_infracao
                        in (select id_auto_infracao from autuacoes_inclusao)
                    )
                    or (
                        a.data_pagamento is not null
                        and (
                            a.id_auto_infracao
                            in (select id_auto_infracao from autuacoes_inclusao)
                            {% if is_incremental() %}
                                or a.id_auto_infracao in (
                                    select contrato
                                    from {{ this }}
                                    where data in ({{ partitions | join(", ") }})
                                )
                            {% endif %}
                        )
                    )
                )
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
                indicador_nao_inclusao as indicador_nao_inclusao_atual,
                motivo as motivo_atual,
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
                cast(null as bool) as indicador_nao_inclusao_atual,
                cast(null as string) as motivo_atual,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),

    sha_dados_completos as (
        select
            n.*,
            a.* except (contrato),
            case
                when
                    a.contrato is not null
                    and a.data_inclusao_atual != n.data_lote
                    and a.indicador_nao_inclusao_atual is false
                    and a.data_baixa_atual is null
                then 'SMTR - AUTUAÇÃO JÁ NEGATIVADA'
                when n.data_pagamento is not null and a.contrato is null
                then 'SMTR - AUTUAÇÃO PAGA'
            end as motivo_calculado
        from sha_dados_novos n
        left join sha_dados_atuais a using (contrato)
    ),

    dados_novos_com_controle as (
        select
            data,
            coalesce(data_inclusao_atual, data_lote) as data_inclusao,
            case
                when
                    data_pagamento is not null
                    and data_inclusao_atual is not null
                    and indicador_nao_inclusao_atual is false
                then coalesce(data_baixa_atual, current_date("America/Sao_Paulo"))
            end as data_baixa,
            case
                when indicador_nao_inclusao_atual is true
                then true
                when motivo_calculado is not null
                then true
                else false
            end as indicador_nao_inclusao,
            coalesce(motivo_atual, motivo_calculado) as motivo,
            nome,
            cpf,
            cnpj,
            endereco,
            bairro,
            cidade,
            cep,
            estado,
            contrato,
            datavencimento,
            datavenda,
            valor,
            valor_pago,
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
