{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

{% set datetime_transacao_column %}
    ifnull(
        dt_transacao,
        datetime(
            timestamp_millis(
                cast(json_value(tx_transacao, '$.dataTransacao') as int)
            ),
            "America/Sao_Paulo"
        )
    )
{% endset %}

{% set staging_transacao_erro = ref("staging_transacao_erro") %}

{% if is_incremental() and execute %}
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
        select distinct concat("'", date(ifnull({{ datetime_transacao_column }}, dt_inclusao)), "'") as dt_lancamento
        from {{ staging_transacao_erro }}
        where {{ incremental_filter }}
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}


with
    tipo_transacao as (
        select chave as id_tipo_transacao, valor as tipo_transacao
        from {{ ref("dicionario_bilhetagem") }}
        where id_tabela = "transacao" and coluna = "id_tipo_transacao"
    ),
    tipo_pagamento as (
        select chave as id_tipo_pagamento, valor as tipo_pagamento
        from {{ ref("dicionario_bilhetagem") }}
        where id_tabela = "transacao" and coluna = "id_tipo_pagamento"
    ),
    aplicacao as (
        select chave as id_aplicacao, valor as aplicacao
        from {{ ref("dicionario_bilhetagem") }}
        where id_tabela = "transacao_erro" and coluna = "id_aplicacao"
    ),
    staging as (
        select *
        from {{ ref("staging_transacao_erro") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by id_transacao_recebida order by timestamp_captura desc
            )
            = 1
    ),
    transacao_erro_json_desaninhado as (
        select
            id_transacao_recebida,
            {{ datetime_transacao_column }} as datetime_transacao,
            dt_inclusao as datetime_inclusao,
            timestamp_captura as datetime_captura,
            tx_erro as erro,
            tx_complemento_erro as complemento_erro,
            cast(json_value(tx_transacao, '$.idTipoModal') as string) as id_tipo_modal,
            cast(json_value(tx_transacao, '$.idConsorcio') as string) as id_consorcio,
            cast(json_value(tx_transacao, '$.idOperadora') as string) as id_operadora,
            cast(json_value(tx_transacao, '$.idLinha') as string) as id_linha,
            cast(
                json_value(tx_transacao, '$.sentidoViagem') as string
            ) as sentido_viagem,
            cast(json_value(tx_transacao, '$.idVeiculo') as string) as id_veiculo,
            cast(
                json_value(tx_transacao, '$.numeroSerieValidador') as string
            ) as numero_serie_validador,
            cast(json_value(tx_transacao, '$.idCliente') as string) as id_cliente,
            cast(json_value(tx_transacao, '$.panHash') as string) as pan_hash,
            cast(json_value(tx_transacao, '$.valorSaldo') as numeric) as valor_saldo,
            cast(json_value(tx_transacao, '$.tipoMidia') as string) as tipo_midia,
            cast(json_value(tx_transacao, '$.idProduto') as string) as id_produto,
            cast(json_value(tx_transacao, '$.idAplicacao') as string) as id_aplicacao,
            cast(
                json_value(tx_transacao, '$.tokenTransacaoEmv') as string
            ) as token_transacao_emv,
            cast(json_value(tx_transacao, '$.EMV') as bool) as emv,
            id_tipo_transacao,
            cast(json_value(tx_transacao, '$.latitude') as float64) as latitude,
            cast(json_value(tx_transacao, '$.longitude') as float64) as longitude,
            cast(
                json_value(tx_transacao, '$.valorTransacao') as numeric
            ) as valor_transacao,
            tx_transacao as json_transacao
        from staging
    ),
    dados_novos as (
        select
            date(ifnull(t.datetime_transacao, t.datetime_inclusao)) as data,
            t.id_transacao_recebida,
            t.datetime_inclusao,
            t.datetime_captura,
            t.erro,
            t.complemento_erro,
            t.id_tipo_modal as id_modo,
            m.modo,
            t.id_consorcio,
            dc.consorcio,
            o.id_operadora,
            t.id_operadora as id_operadora_jae,
            o.operadora,
            o.documento as documento_operadora,
            o.tipo_documento as tipo_documento_operadora,
            t.id_linha as id_servico_jae,
            s.servico_jae,
            s.descricao_servico_jae,
            t.sentido_viagem as sentido,
            case
                when m.modo = "VLT"
                then substring(t.id_veiculo, 1, 3)
                when m.modo = "BRT"
                then null
                else t.id_veiculo
            end as id_veiculo,
            t.numero_serie_validador as id_validador,
            t.id_cliente as id_cliente,
            t.pan_hash as hash_cartao,
            t.valor_saldo as saldo_cartao,
            t.tipo_midia as id_tipo_pagamento,
            tp.tipo_pagamento as meio_pagamento_jae,
            t.id_produto,
            p.nm_produto as produto_jae,
            t.id_aplicacao,
            ap.aplicacao,
            t.token_transacao_emv,
            t.emv as indicador_emv,
            t.id_tipo_transacao,
            tt.tipo_transacao as tipo_transacao_jae,
            t.latitude,
            t.longitude,
            t.valor_transacao,
            t.json_transacao
        from transacao_erro_json_desaninhado t
        left join
            {{ ref("modos") }} m on t.id_tipo_modal = m.id_modo and m.fonte = "jae"
        left join {{ ref("operadoras") }} o on t.id_operadora = o.id_operadora_jae
        left join {{ ref("consorcios") }} dc on t.id_consorcio = dc.id_consorcio_jae
        left join
            {{ ref("aux_servico_jae") }} s
            on t.id_linha = s.id_servico_jae
            and t.datetime_transacao >= s.datetime_inicio_validade
            and (
                t.datetime_transacao < s.datetime_fim_validade
                or s.datetime_fim_validade is null
            )
        left join {{ ref("staging_produto") }} p on t.id_produto = p.cd_produto
        left join tipo_transacao tt on tt.id_tipo_transacao = t.id_tipo_transacao
        left join tipo_pagamento tp on t.tipo_midia = tp.id_tipo_pagamento
        left join aplicacao ap on t.id_aplicacao = ap.id_aplicacao
    ),
    {% if is_incremental() %}

        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} 1 = 0
                {% endif %}

        ),
    {% endif %}
    particoes_completas as (
        select *, 0 as priority
        from dados_novos

        {% if is_incremental() %}
            union all

            select
                * except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais

        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from particoes_completas
        qualify
            row_number() over (
                partition by id_transacao_recebida
                order by priority, datetime_captura desc
            )
            = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_transacao_recebida,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_transacao_recebida,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_transacao_recebida)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_transacao_recebida)
    ),
    transacao_erro_colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual,
                priority
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
from transacao_erro_colunas_controle
