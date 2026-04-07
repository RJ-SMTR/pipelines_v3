{{
    config(
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set integracao_table = ref("integracao") %}
{% if execute and is_incremental() %}
    {% set partitions_query %}
        select concat("'", parse_date("%Y%m%d", partition_id), "'") as data
        from
            `{{ integracao_table.database }}.{{ integracao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = "{{ integracao_table.identifier }}"
            and partition_id != "__NULL__"
            and date(last_modified_time, "America/Sao_Paulo")
            between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% set adjacent_partitions_query %}
        with base as (
            select
                parse_date("%Y%m%d", partition_id) as data
            from
                `{{ integracao_table.database }}.{{ integracao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ integracao_table.identifier }}"
                and partition_id != "__NULL__"
                and date(last_modified_time, "America/Sao_Paulo")
                    between date("{{ var('date_range_start') }}")
                    and date("{{ var('date_range_end') }}")
        )
        select concat("'", data, "'") as data
        from (
            select date_sub(data, interval 1 day) as data from base
            union distinct
            select date_add(data, interval 1 day) from base
        )
    {% endset %}

    {% set adjacent_partitions = (
        run_query(adjacent_partitions_query).columns[0].values()
    ) %}

    {% if partitions | length > 0 %}
        {% set final_partitions_query %}
            select distinct concat("'", date(datetime_processamento_integracao), "'")
            from {{ integracao_table }}
            where
                data in ({{ partitions | join(", ") }})
                or data in ({{ adjacent_partitions | join(", ") }})
        {% endset %}

        {% set final_partitions = (
            run_query(final_partitions_query).columns[0].values()
        ) %}

    {% else %} {% set final_partitions = [] %}
    {% endif %}

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
                    {% if c in ("rateio_realizado", "rateio_matriz", "transferencias_realizadas", "datas_transacoes") %}
                        ifnull(to_json_string({{ c }}), 'n/a')
                    {%else%}
                        ifnull(cast({{ c }} as string), 'n/a')
                    {% endif %}
                    {% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        )
    {% endset %}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
    {% set partitions = [] %}
{% endif %}

with
    matriz_integracao as (select * from {{ ref("matriz_integracao") }}),
    integracao_particao_modificada as (
        select date(cast(p as string)) as particao
        from unnest([{{ partitions | join(", ") }}]) as p
    ),
    integracao as (
        /*
        1. Altera a informação de modo para do padrão da Jaé para o padrão da matriz
        2. Filtra partições modificadas
        */
        select
            i.*,
            case
                when
                    modo = 'Ônibus'
                    and array_length(regexp_extract_all(servico_jae, r'[0-9]')) = 3
                then 'SPPO'
                when modo = 'Van'
                then consorcio
                else modo
            end as modo_tratado
        from {{ integracao_table }} i
        left join integracao_particao_modificada pm on i.data = pm.particao
        where
        {% if is_incremental() %}
                {% if partitions | length > 0 %}
                    data in ({{ partitions | join(", ") }})
                    or data in ({{ adjacent_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
            qualify max(pm.particao is not null) over (partition by id_integracao)
            {% else %} data >= "2000-01-01"
            {% endif %}
    ),
    integracao_origem_destino as (
        /*
        Altera o formato da tabela de integração adicionando informações da transação de destino
        */
        select
            date(datetime_processamento_integracao) as data,
            data as data_transacao,
            lead(data) over (win) as data_lead,
            id_integracao,
            sequencia_integracao,
            modo_tratado as modo_origem,
            id_servico_jae as id_servico_jae_origem,
            servico_jae as servico_jae_origem,
            lead(modo_tratado) over (win) as modo_destino,
            lead(id_servico_jae) over (win) as id_servico_jae_destino,
            lead(servico_jae) over (win) as servico_jae_destino,
            datetime_transacao as datetime_transacao_origem,
            lead(datetime_transacao) over (win) as datetime_transacao_destino,
            round(
                ifnull(cast(percentual_rateio as numeric), 0), 2
            ) as percentual_rateio_origem,
            round(
                ifnull(lead(cast(percentual_rateio as numeric)) over (win), 0), 2
            ) as percentual_rateio_destino
        from integracao
        window win as (partition by id_integracao order by sequencia_integracao)
    ),
    integracao_sem_transferencia as (
        /*
        1. Cria coluna integracao_origem seguindo a lógica da matriz
        2. Remove transferências
        3. Remove linha sem transação de destino (última transação da integração)
        */
        select
            *,
            case
                when row_number() over (win) > 1
                then
                    string_agg(modo_origem, '-') over (
                        partition by id_integracao
                        order by sequencia_integracao
                        rows between unbounded preceding and current row
                    )
            end as integracao_origem,
            row_number() over (win) as rn
        from integracao_origem_destino
        where
            (modo_origem not in ('BRT', 'VLT') or modo_origem != modo_destino)
            and modo_destino is not null
        window win as (partition by id_integracao order by sequencia_integracao)
    ),
    integracao_matriz as (
        /*
        1. Faz o join com a matriz
        2. Cria um array com informações da integração para ser tratado posteriormente
        */
        select
            i.*,
            m.integracao is null as indicador_integracao_fora_matriz,
            m.tempo_integracao_minutos,
            case
                when rn = 1  -- a primeira transação com origem e destino
                then
                    [
                        struct(
                            percentual_rateio_origem as percentual_rateio,
                            i.modo_origem as modo,
                            1 as ordem
                        ),
                        struct(
                            percentual_rateio_destino as percentual_rateio,
                            i.modo_destino as modo,
                            2 as ordem
                        )
                    ]
                else  -- transações seguintes somente destino (a origem é igual ao destino da transação anterior)
                    [
                        struct(
                            percentual_rateio_destino as percentual_rateio,
                            i.modo_destino as modo,
                            1 as ordem
                        )
                    ]
            end as array_integracao
        from integracao_sem_transferencia i
        left join
            matriz_integracao m
            on i.data_lead >= m.data_inicio
            and (i.data_lead <= m.data_fim or m.data_fim is null)
            and if(sequencia_integracao = 1, i.modo_origem, '')
            = ifnull(m.modo_origem, '')
            and (
                i.id_servico_jae_origem = m.id_servico_jae_origem
                or m.id_servico_jae_origem is null
            )
            and i.modo_destino = m.modo_destino
            and (
                i.id_servico_jae_destino = m.id_servico_jae_destino
                or m.id_servico_jae_destino is null
            )
            and ifnull(i.integracao_origem, '') = ifnull(m.integracao_origem, '')
    ),
    integracao_array_tratado as (
        /*
        Faz o tratamento do array gerado anteriormente em uma tabela auxiliar
        */
        select
            id_integracao,
            array_agg(distinct data_transacao) as datas_transacoes,
            array_agg(a.percentual_rateio order by rn, a.ordem) as rateio_realizado,
            string_agg(a.modo, '-' order by rn, a.ordem) as integracao_realizada
        from integracao_matriz, unnest(array_integracao) as a
        group by 1
    ),
    integracao_agregada as (
        /*
        1. Agrega cada integração em uma linha
        2. Pega informações de rateio da matriz de repartição
        */
        select
            im.data,
            im.id_integracao,
            iat.integracao_realizada,
            max(
                im.indicador_integracao_fora_matriz
            ) as indicador_integracao_fora_matriz,
            im.tempo_integracao_minutos as tempo_integracao_minutos_matriz,
            sum(
                datetime_diff(
                    im.datetime_transacao_destino, im.datetime_transacao_origem, minute
                )
            ) as tempo_integracao_minutos_realizado,
            iat.rateio_realizado,
            array(
                select round(s, 2) from unnest(mrt.sequencia_rateio) s
            ) as rateio_matriz,
            datas_transacoes,
            "Integração" as tipo_integracao
        from integracao_matriz im
        join integracao_array_tratado iat using (id_integracao)
        left join
            {{ ref("matriz_reparticao_tarifaria") }} mrt
            on mrt.data_inicio_matriz <= im.data_lead
            and (mrt.data_fim_matriz >= im.data_lead or mrt.data_fim_matriz is null)
            and mrt.integracao = iat.integracao_realizada
        group by all
    ),
    transferencia as (
        /*
        Filtra somente as transferências
        */
        select
            *,
            row_number() over (
                partition by id_integracao order by sequencia_integracao
            ) as rn
        from integracao_origem_destino
        where (modo_origem in ('BRT', 'VLT') and modo_origem = modo_destino)
    ),
    mudanca_transferencia as (
        /*
        Classifica as transferências em agrupamentos
        */
        select
            *,
            case
                when
                    sequencia_integracao = lag(sequencia_integracao) over (
                        partition by id_integracao order by rn
                    )
                    + 1
                then 0
                else 1
            end as indicador_mudanca_transferencia
        from transferencia
    ),
    transferencia_id as (
        /*
        Identifica as transferências dentro de uma integração
        */
        select
            sum(indicador_mudanca_transferencia) over (
                partition by id_integracao order by rn
            ) as id_transferencia,
            *
        from mudanca_transferencia
    ),
    transferencia_matriz as (
        /*
        1. Faz o join com a matriz
        2. Cria um array com informações da transferência para ser tratado posteriormente
        */
        select
            t.*,
            m.integracao is null as indicador_transferencia_fora_matriz,
            m.tempo_integracao_minutos,
            case
                when rn = 1  -- a primeira transação com origem e destino
                then
                    [
                        struct(t.modo_origem as modo, 1 as ordem),
                        struct(t.modo_destino as modo, 2 as ordem)
                    ]
                else [struct(t.modo_destino as modo, 1 as ordem)]  -- transações seguintes somente destino (a origem é igual ao destino da transação anterior)
            end as array_modo
        from transferencia_id t
        left join
            matriz_integracao m
            on t.data_lead >= m.data_inicio
            and (t.data_lead <= m.data_fim or m.data_fim is null)
            and t.modo_origem = m.modo_origem
            and (
                t.id_servico_jae_origem = m.id_servico_jae_origem
                or m.id_servico_jae_origem is null
            )
            and t.modo_destino = m.modo_destino
            and (
                t.id_servico_jae_destino = m.id_servico_jae_destino
                or m.id_servico_jae_destino is null
            )
    ),
    transferencia_array_tratado as (
        /*
        Faz o tratamento do array gerado anteriormente em uma tabela auxiliar
        */
        select
            id_integracao,
            id_transferencia,
            array_agg(distinct data_transacao) as datas_transacoes,
            string_agg(a.modo, '-' order by rn, a.ordem) as integracao_realizada
        from transferencia_matriz, unnest(array_modo) as a
        group by 1, 2
    ),
    transferencia_agregada as (
        /*
        Agrega cada transferência em uma única linha
        */
        select
            tmt.data,
            tmt.id_integracao,
            tmt.id_transferencia,
            tat.integracao_realizada,
            max(
                tmt.indicador_transferencia_fora_matriz
            ) as indicador_transferencia_fora_matriz,
            tmt.tempo_integracao_minutos as tempo_transferencia_minutos_matriz,
            sum(
                datetime_diff(
                    tmt.datetime_transacao_destino,
                    tmt.datetime_transacao_origem,
                    minute
                )
            ) as tempo_transferencia_minutos_realizado,
            datas_transacoes,
            "Transferência" as tipo_integracao
        from transferencia_matriz tmt
        join transferencia_array_tratado tat using (id_integracao, id_transferencia)
        group by all
    ),
    validacao_transferencia as (
        /*
        Cria indicadores de validacao das transferências
        */
        select
            data,
            id_integracao,
            array_agg(
                struct(
                    integracao_realizada as transferencia_realizada,
                    tempo_transferencia_minutos_matriz
                    as tempo_transferencia_minutos_matriz,
                    tempo_transferencia_minutos_realizado
                    as tempo_transferencia_minutos_realizado
                )
            ) as transferencias_realizadas,
            max(
                indicador_transferencia_fora_matriz
            ) as indicador_transferencia_fora_matriz,
            max(
                tempo_transferencia_minutos_matriz
                < tempo_transferencia_minutos_realizado
            ) as indicador_tempo_transferencia_invalido,
            array_concat_agg(datas_transacoes) as datas_transacoes,
        from transferencia_agregada
        group by 1, 2
    ),
    integracao_transferencia as (
        /*
        1. Junta informações de integração com as de transferência
        2. Cria indicadores de validação das integrações
        */
        select
            data,
            id_integracao,
            i.integracao_realizada,
            i.tempo_integracao_minutos_realizado,
            i.tempo_integracao_minutos_matriz,
            i.rateio_realizado,
            i.rateio_matriz,
            t.transferencias_realizadas,
            i.indicador_integracao_fora_matriz,
            i.tempo_integracao_minutos_matriz
            < i.tempo_integracao_minutos_realizado
            as indicador_tempo_integracao_invalido,
            to_json_string(i.rateio_realizado)
            != to_json_string(i.rateio_matriz) as indicador_rateio_invalido,
            t.indicador_transferencia_fora_matriz,
            t.indicador_tempo_transferencia_invalido,
            array(
                select di
                from unnest(i.datas_transacoes) as di
                union distinct
                select dt
                from unnest(t.datas_transacoes) as dt
            ) as datas_transacoes
        from integracao_agregada i
        full outer join validacao_transferencia t using (data, id_integracao)
    ),
    {% if is_incremental() %}
        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if final_partitions | length > 0 %}
                    data in ({{ final_partitions | join(", ") }})
                {% else %} false
                {% endif %}

        ),
    {% endif %}
    particoes_completas as (
        select *, 0 as priority
        from integracao_transferencia

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
            row_number() over (partition by id_integracao order by priority, data desc)
            = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                id_integracao,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_integracao,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_integracao)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_integracao)
    ),
    integracao_invalida_colunas_controle as (
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
from integracao_invalida_colunas_controle
where
    indicador_integracao_fora_matriz
    or indicador_tempo_integracao_invalido
    or indicador_rateio_invalido
    or indicador_transferencia_fora_matriz
    or indicador_tempo_transferencia_invalido
