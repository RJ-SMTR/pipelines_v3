{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        require_partition_filter=true,
    )
}}

{% set transacao_ordem = ref("aux_transacao_id_ordem_pagamento") %}
{% set aux_transacao_particao = ref("aux_transacao_particao") %}
{% set transacao_retificada = ref("transacao_retificada") %}


{% set incremental_filter %}
    ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

-- busca quais partições serão atualizadas pelas capturas
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
                        {% if c == "geo_point_transacao" %}
                            ifnull(st_astext(geo_point_transacao), 'n/a')
                        {% elif c == "hash_cliente" %}
                            ifnull(to_base64(hash_cliente), 'n/a')
                        {% else %}ifnull(cast({{ c }} as string), 'n/a')
                        {% endif %}

                        {% if not loop.last %}, {% endif %}

                    {% endfor %}
                )
            )
    {% endset %}
    {% set transacao_partitions_query %}
            select distinct
                concat("'", particao, "'") as particao
            from
                (
                    select
                        array_concat_agg(particoes) as particoes
                    from
                        {{ aux_transacao_particao }}
                    where {{ incremental_filter }}
                ),
                unnest(particoes) as particao

            union distinct

            select
                concat("'", parse_date("%Y%m%d", partition_id), "'") as particao
            from
                `{{ transacao_ordem.database }}.{{ transacao_ordem.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ transacao_ordem.identifier }}"
                and partition_id != "__NULL__"
                and
                    datetime(last_modified_time, "America/Sao_Paulo")
                    between datetime_add(datetime("{{var('date_range_start')}}"), interval 1 hour)
                    and datetime_add(datetime("{{var('date_range_end')}}"), interval 1 hour)

            union distinct

            select
                concat("'", parse_date("%Y%m%d", partition_id), "'") as particao
            from
                `{{ transacao_retificada.database }}.{{ transacao_retificada.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ transacao_retificada.identifier }}"
                and partition_id != "__NULL__"
                and
                    datetime(last_modified_time, "America/Sao_Paulo")
                    between datetime_add(datetime("{{var('date_range_start')}}"), interval 1 hour)
                    and datetime_add(datetime("{{var('date_range_end')}}"), interval 1 hour)

    {% endset %}

    {% set transacao_partitions = (
        run_query(transacao_partitions_query).columns[0].values()
    ) %}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    transacao_staging as (
        select *
        from {{ ref("staging_transacao") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    tipo_transacao as (
        select chave as id_tipo_transacao, valor as tipo_transacao
        from {{ ref("dicionario_bilhetagem") }}
        where id_tabela = "transacao" and coluna = "id_tipo_transacao"
    ),
    tipo_documento as (
        select chave as cd_tipo_documento, valor as tipo_documento
        from {{ ref("dicionario_bilhetagem") }}
        where id_tabela = "cliente" and coluna = "cd_tipo_documento"
    ),
    gratuidade as (
        select
            cast(id_cliente as string) as id_cliente,
            tipo_gratuidade,
            rede_ensino,
            id_cre_escola,
            deficiencia_permanente,
            datetime_inicio_validade,
            datetime_fim_validade
        from {{ ref("aux_gratuidade_info") }}
    ),
    tipo_pagamento as (
        select chave as id_tipo_pagamento, valor as tipo_pagamento
        from {{ ref("dicionario_bilhetagem") }}
        {# from `rj-smtr.bilhetagem.dicionario` #}
        where id_tabela = "transacao" and coluna = "id_tipo_pagamento"
    ),
    integracao as (
        select id_transacao, valor_rateio, datetime_processamento_integracao
        from {{ ref("integracao") }}
        {# from `rj-smtr.bilhetagem.integracao` #}
        {% if is_incremental() %}
            where
                {% if transacao_partitions | length > 0 %}
                    data in ({{ transacao_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        {% endif %}
        qualify
            row_number() over (
                partition by id_transacao
                order by datetime_processamento_integracao desc
            )
            = 1
    ),
    transacao_ordem as (
        select *
        from {{ ref("aux_transacao_id_ordem_pagamento") }}
        {% if is_incremental() %}
            where
                {% if transacao_partitions | length > 0 %}
                    data in ({{ transacao_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        {% endif %}
    ),
    transacao_nova as (
        select
            extract(date from data_transacao) as data,
            extract(hour from data_transacao) as hora,
            id as id_transacao,
            data_transacao as datetime_transacao,
            data_processamento as datetime_processamento,
            t.timestamp_captura as datetime_captura,
            m.modo,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            t.cd_operadora as id_operadora_jae,
            do.operadora,
            do.documento as documento_operadora,
            do.tipo_documento as tipo_documento_operadora,
            s.id_servico_jae,
            s.servico_jae,
            s.descricao_servico_jae,
            sentido,
            case
                when m.modo = "VLT"
                then substring(t.veiculo_id, 1, 3)
                when m.modo = "BRT"
                then null
                else t.veiculo_id
            end as id_veiculo,
            t.numero_serie_validador as id_validador,
            t.id_cliente as id_cliente,
            sha256(t.id_cliente) as hash_cliente,
            c.documento as documento_cliente,
            c.tipo_documento as tipo_documento_cliente,
            t.pan_hash as hash_cartao,
            t.vl_saldo as saldo_cartao,
            tp.tipo_pagamento as meio_pagamento_jae,
            p.nm_produto as produto_jae,
            tt.tipo_transacao as tipo_transacao_jae,
            latitude_trx as latitude,
            longitude_trx as longitude,
            st_geogpoint(longitude_trx, latitude_trx) as geo_point_transacao,
            valor_transacao
        from transacao_staging as t
        left join
            {{ ref("modos") }} m on t.id_tipo_modal = m.id_modo and m.fonte = "jae"
        left join {{ ref("operadoras") }} do on t.cd_operadora = do.id_operadora_jae
        left join {{ ref("consorcios") }} dc on t.cd_consorcio = dc.id_consorcio_jae
        left join
            {{ ref("aux_servico_jae") }} s
            on t.cd_linha = s.id_servico_jae
            and t.data_transacao >= s.datetime_inicio_validade
            and (
                t.data_transacao < s.datetime_fim_validade
                or s.datetime_fim_validade is null
            )
        left join {{ ref("staging_produto") }} p on t.id_produto = p.cd_produto
        left join {{ ref("cliente_jae") }} c using (id_cliente)
        left join tipo_transacao tt on tt.id_tipo_transacao = t.tipo_transacao
        left join tipo_pagamento tp on t.id_tipo_midia = tp.id_tipo_pagamento
        left join
            {{ ref("staging_linha_sem_ressarcimento") }} lsr
            on t.cd_linha = lsr.id_linha
        where lsr.id_linha is null and date(data_transacao) >= "2023-07-17"
    ),
    {% if is_incremental() %}
        transacao_atual as (
            select *
            from {{ this }}
            where
                {% if transacao_partitions | length > 0 %}
                    data in ({{ transacao_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        ),
    {% endif %}
    particao_completa as (
        select
            data,
            hora,
            id_transacao,
            datetime_transacao,
            datetime_processamento,
            datetime_captura,
            modo,
            id_consorcio,
            consorcio,
            id_operadora,
            id_operadora_jae,
            operadora,
            documento_operadora,
            tipo_documento_operadora,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            sentido,
            id_veiculo,
            id_validador,
            id_cliente,
            hash_cliente,
            documento_cliente,
            tipo_documento_cliente,
            hash_cartao,
            saldo_cartao,
            meio_pagamento_jae,
            produto_jae,
            tipo_transacao_jae,
            latitude,
            longitude,
            geo_point_transacao,
            valor_transacao,
            0 as priority
        from transacao_nova

        {% if is_incremental() %}
            union all

            select
                data,
                hora,
                id_transacao,
                datetime_transacao,
                datetime_processamento,
                datetime_captura,
                modo,
                id_consorcio,
                consorcio,
                id_operadora,
                id_operadora_jae,
                operadora,
                documento_operadora,
                tipo_documento_operadora,
                id_servico_jae,
                servico_jae,
                descricao_servico_jae,
                sentido,
                id_veiculo,
                id_validador,
                id_cliente,
                hash_cliente,
                documento_cliente,
                tipo_documento_cliente,
                hash_cartao,
                saldo_cartao,
                meio_pagamento_jae,
                produto_jae,
                tipo_transacao_jae,
                latitude,
                longitude,
                geo_point_transacao,
                valor_transacao,
                1 as priority
            from transacao_atual
        {% endif %}
    ),
    transacao_retificada as (
        select *
        from {{ transacao_retificada }}
        {% if is_incremental() %}
            where
                {% if transacao_partitions | length > 0 %}
                    data in ({{ transacao_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        {% endif %}
        qualify
            row_number() over (
                partition by id_transacao order by datetime_retificacao desc
            )
            = 1

    ),
    particao_completa_deduplicada as (
        select * except (priority)
        from particao_completa
        qualify row_number() over (partition by id_transacao order by priority) = 1
    ),
    retificacao_transacao as (
        select
            p.* except (tipo_transacao_jae, valor_transacao),
            ifnull(
                tr.tipo_transacao_jae_retificada, p.tipo_transacao_jae
            ) as tipo_transacao_jae,
            ifnull(tr.valor_transacao_retificada, p.valor_transacao) as valor_transacao
        from particao_completa_deduplicada p
        left join transacao_retificada tr using (id_transacao)
    ),
    -- Adiciona informações que são modificadas posteriormente pelo processo da Jaé
    transacao_info_posterior as (
        select
            t.*,
            case
                when
                    t.tipo_transacao_jae in ("Integração", "Integração EMV")
                    or (
                        i.id_transacao is not null
                        and t.tipo_transacao_jae
                        not in ("Transferência EMV", "Transferência")
                    )
                then "Integração"
                when
                    t.tipo_transacao_jae in (
                        "Débito", "Débito EMV", "Débito EMV emissor externo", "Botoeira"
                    )
                then "Integral"
                when t.tipo_transacao_jae = "Transferência EMV"
                then "Transferência"
                when
                    t.tipo_transacao_jae in (
                        "Gratuidade acompanhante",
                        "Gratuidade operadora",
                        "Gratuidade operador sênior",
                        "Gratuidade operador pcd",
                        "Gratuidade operador estudante",
                        "Gratuidade operador menor 5 anos",
                        "Gratuidade operador policial"
                    )
                then "Gratuidade"
                else t.tipo_transacao_jae
            end as tipo_transacao_atualizado,
            case
                when
                    i.id_transacao is not null
                    or o.id_transacao is not null
                    or date(t.datetime_processamento)
                    < (select max(data_ordem) from {{ ref("bilhetagem_dia") }})
                then coalesce(i.valor_rateio, t.valor_transacao) * 0.96
            end as valor_pagamento,
            o.data_ordem,
            o.id_ordem_pagamento_servico_operador_dia,
            o.id_ordem_pagamento_consorcio_operador_dia,
            o.id_ordem_pagamento_consorcio_dia,
            o.id_ordem_pagamento
        from retificacao_transacao t
        left join integracao i using (id_transacao)
        left join transacao_ordem o using (id_transacao)
    ),
    -- Cria os níveis de classificação da transação
    transacao_nivel as (
        select
            t.*,
            case
                when t.id_cliente is null or t.id_cliente = "733"
                then "Não Cadastrado"
                else "Cadastrado"
            end as cadastro_cliente,
            case
                when t.produto_jae in ("Conta Jaé", "Conta digital")
                then "Carteira"
                when t.produto_jae = "Conta Jaé Gratuidade"
                then "Gratuidade"
                when t.produto_jae = "Conta Jaé VT"
                then "VT"
                when t.produto_jae = "Avulso"
                then "Cartão Avulso"
                when
                    t.tipo_transacao_jae like "% EMV %"
                    or t.tipo_transacao_jae like "% EMV"
                then "Visa Internacional"
                when t.tipo_transacao_jae = "Botoeira"
                then "Dinheiro (Botoeira)"
                when tipo_transacao_jae like "Gratuidade operador%"
                then "Gratuidade Operadora"
            end as produto,
            case
                when t.produto_jae = "Conta Jaé Gratuidade"
                then "Gratuidade"
                else t.tipo_transacao_atualizado
            end as tipo_transacao,
            case
                when
                    t.tipo_transacao_jae not like "%Gratuidade%"
                    and (
                        t.produto_jae != "Conta Jaé Gratuidade" or t.produto_jae is null
                    )
                then "Pagante"
                when
                    t.tipo_transacao_jae
                    in ("Gratuidade operador pcd", "Gratuidade acompanhante")
                    or g.tipo_gratuidade = "PCD"
                then "Saúde"
                when t.tipo_transacao_jae = "Gratuidade operador estudante"
                then "Estudante"
                when
                    g.tipo_gratuidade = "Sênior"
                    or t.tipo_transacao_jae = "Gratuidade operador sênior"
                then "Idoso"
                when tipo_transacao_jae like "Gratuidade operador%"
                then "Operadora"
                else g.tipo_gratuidade
            end as tipo_usuario,
            case
                when
                    t.tipo_transacao_jae = "Gratuidade acompanhante"
                    or (
                        t.tipo_transacao_jae not like "%Gratuidade%"
                        and (
                            t.produto_jae != "Conta Jaé Gratuidade"
                            or t.produto_jae is null
                        )

                    )
                then null
                when
                    g.tipo_gratuidade = "Estudante"
                    and g.rede_ensino like "Universidade%"
                then "Ensino Superior"
                when g.tipo_gratuidade = "Estudante" and g.rede_ensino is not null
                then concat("Ensino Básico ", split(g.rede_ensino, " - ")[0])
            end as subtipo_usuario,
            case
                when t.tipo_transacao_jae = "Gratuidade acompanhante"
                then "Acompanhante"
                when
                    t.tipo_transacao_jae != "Gratuidade"
                    and t.produto_jae != "Conta Jaé Gratuidade"
                then null
                when
                    g.tipo_gratuidade = "Estudante"
                    and g.rede_ensino like "Universidade%"
                then concat("Ensino Superior ", g.rede_ensino)
                when g.tipo_gratuidade = "Estudante" and g.rede_ensino is not null
                then concat("Ensino Básico ", split(g.rede_ensino, " - ")[0])
                when g.tipo_gratuidade = "PCD" and g.deficiencia_permanente
                then "PCD"
                when g.tipo_gratuidade = "PCD" and not g.deficiencia_permanente
                then "DC"
            end as subtipo_usuario_protegido,
            case
                when t.tipo_transacao_jae = "Botoeira"
                then "Dinheiro"
                when t.meio_pagamento_jae like "Cartão%"
                then "Cartão"
                else t.meio_pagamento_jae
            end as meio_pagamento,
            g.id_cre_escola
        from transacao_info_posterior t
        left join
            gratuidade g
            on t.id_cliente = g.id_cliente
            and t.datetime_transacao >= g.datetime_inicio_validade
            and (
                t.datetime_transacao < g.datetime_fim_validade
                or g.datetime_fim_validade is null
            )
    ),
    transacao_colunas_ordenadas as (
        select
            data,
            hora,
            id_transacao,
            datetime_transacao,
            datetime_processamento,
            datetime_captura,
            modo,
            id_consorcio,
            consorcio,
            id_operadora,
            id_operadora_jae,
            operadora,
            documento_operadora,
            tipo_documento_operadora,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            sentido,
            id_veiculo,
            id_validador,
            id_cliente,
            hash_cliente,
            documento_cliente,
            tipo_documento_cliente,
            hash_cartao,
            saldo_cartao,
            cadastro_cliente,
            produto,
            produto_jae,
            tipo_transacao,
            tipo_transacao_jae,
            tipo_usuario,
            subtipo_usuario,
            subtipo_usuario_protegido,
            meio_pagamento,
            meio_pagamento_jae,
            id_cre_escola,
            latitude,
            longitude,
            geo_point_transacao,
            valor_transacao,
            valor_pagamento,
            data_ordem,
            id_ordem_pagamento_servico_operador_dia,
            id_ordem_pagamento_consorcio_operador_dia,
            id_ordem_pagamento_consorcio_dia,
            id_ordem_pagamento
        from transacao_nivel
    ),

    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo from transacao_colunas_ordenadas
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_transacao,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from transacao_atual

        {% else %}
            select
                cast(null as string) as id_transacao,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select * from sha_dados_novos left join sha_dados_atuais using (id_transacao)
    ),
    transacao_colunas_controle as (
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
from transacao_colunas_controle
