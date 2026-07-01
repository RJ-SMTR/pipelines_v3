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
            ifnull(
                sm.modos,
                case
                    when i.modo = 'Van'
                    then [consorcio]
                    when
                        i.modo = 'Ônibus'
                        and not (
                            length(ifnull(regexp_extract(i.servico_jae, r'[0-9]+'), ''))
                            = 4
                            and ifnull(regexp_extract(i.servico_jae, r'[0-9]+'), '')
                            like '2%'
                            and length(
                                ifnull(regexp_extract(i.servico_jae, r'[0-9]+'), '')
                            )
                            = 2
                        )
                    then ['SPPO']
                    when
                        i.modo = 'BRT'
                        and ifnull(l.tarifa_ida, l.tarifa_volta) > tp.valor_tarifa
                    then ['BRT ESP']
                    else [i.modo]
                end
            ) as modos
        from {{ integracao_table }} i
        left join integracao_particao_modificada pm on i.data = pm.particao
        join
            {{ ref("tarifa_publica") }} tp
            on i.data >= tp.data_inicio
            and (i.data <= tp.data_fim or tp.data_fim is null)
        left join
            {{ ref("matriz_integracao_servico_modo") }} sm
            on i.id_servico_jae = sm.id_servico_jae
            and i.data >= sm.data_inicio_validade
            and (i.data < sm.data_fim_validade or sm.data_fim_validadeis is null)
        left join
            {{ ref("aux_linha_tarifa") }} l
            on i.id_servico_jae = l.cd_linha
            and i.datetime_transacao >= l.dt_inicio_validade
            and (
                l.data_fim_validade is null
                or i.datetime_transacao < l.data_fim_validade
            )
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
            modos as modos_origem,
            id_servico_jae as id_servico_jae_origem,
            servico_jae as servico_jae_origem,
            lead(modos) over (win) as modos_destino,
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
    integracao_origem_destino_tratado as (
        select
            *,
            concat(
                "(?:", array_to_string(modos_origem, "|"), ")"
            ) as modos_origem_string,
            concat(
                "(?:", array_to_string(modos_destino, "|"), ")"
            ) as modos_destino_string,
            case
                when row_number() over (win) > 1
                then
                    concat(
                        "^",
                        string_agg(
                            concat("(?:", array_to_string(modos_origem, "|"), ")"),
                            '-'
                        ) over (
                            partition by id_integracao
                            order by sequencia_integracao
                            rows between unbounded preceding and current row
                        ),
                        "$"
                    )
            end as integracao_origem_regex,
            row_number() over (win) as rn
        from integracao_origem_destino
        where array_length(modos_destino) > 0
        window win as (partition by id_integracao order by sequencia_integracao)
    ),
    integracao_bum_sem_transferencia as (
        select *
        from integracao_origem_destino_tratado
        where not 'VLT' in unnest(modos_origem) or not 'VLT' in unnest(modos_destino)
    ),
    integracao_bum_matriz as (
        select
            i.*,
            false as indicador_integracao_fora_matriz,
            m.tempo_integracao_minutos,
            case
                when rn = 1  -- a primeira transação com origem e destino
                then
                    [
                        struct(
                            percentual_rateio_origem as percentual_rateio,
                            i.modos_origem_string as modo,
                            1 as ordem
                        ),
                        struct(
                            percentual_rateio_destino as percentual_rateio,
                            i.modos_destino_string as modo,
                            2 as ordem
                        )
                    ]
                else  -- transações seguintes somente destino (a origem é igual ao destino da transação anterior)
                    [
                        struct(
                            percentual_rateio_destino as percentual_rateio,
                            i.modos_destino_string as modo,
                            1 as ordem
                        )
                    ]
            end as array_integracao
        from integracao_bum_sem_transferencia i
        left join
            matriz_integracao m
            on m.tipo_bilhete_unico = 'BUM'
            and m.tipo_integracao != "Transferência"
            and i.data_lead >= m.data_inicio
            and (i.data_lead <= m.data_fim or m.data_fim is null)
            and (
                i.id_servico_jae_origem = m.id_servico_jae_origem
                or m.id_servico_jae_origem is null
            )
            and ifnull(m.modo_destino, '') in unnest(i.modos_destino)
            and (
                i.id_servico_jae_destino = m.id_servico_jae_destino
                or m.id_servico_jae_destino is null
            )
            and (
                (
                    ifnull(m.modo_origem, '') in unnest(i.modos_origem)
                    and i.integracao_origem_regex is null
                )
                or regexp_extract(m.integracao_origem, i.integracao_origem_regex)
                is not null
            )
        qualify max(m.integracao is null) over (partition by id_integracao) = false
    ),
    integracao_buc as (
        select i.*
        from integracao_origem_destino_tratado i
        left join integracao_bum_matriz b using (id_integracao)
        where b.id_integracao is null
    ),
    integracao_buc_sem_transferencia as (
        select *
        from integracao_buc
        where
            (
                not exists (
                    select 1 from unnest(modos_origem) m where m in ('BRT', 'VLT')
                )
                or not exists (
                    select 1
                    from unnest(modos_origem) o
                    join unnest(modos_destino) d on o = d
                )
            )
    ),
    integracao_buc_matriz as (
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
                            i.modos_origem_string as modo,
                            1 as ordem
                        ),
                        struct(
                            percentual_rateio_destino as percentual_rateio,
                            i.modos_destino_string as modo,
                            2 as ordem
                        )
                    ]
                else  -- transações seguintes somente destino (a origem é igual ao destino da transação anterior)
                    [
                        struct(
                            percentual_rateio_destino as percentual_rateio,
                            i.modos_destino_string as modo,
                            1 as ordem
                        )
                    ]
            end as array_integracao
        from integracao_buc_sem_transferencia i
        left join
            matriz_integracao m
            on m.tipo_integracao != "Transferência"
            and m.tipo_bilhete_unico != 'BUM'
            and i.data_lead >= m.data_inicio
            and (i.data_lead <= m.data_fim or m.data_fim is null)
            and (
                i.id_servico_jae_origem = m.id_servico_jae_origem
                or m.id_servico_jae_origem is null
            )
            and ifnull(m.modo_destino, '') in unnest(i.modos_destino)
            and (
                i.id_servico_jae_destino = m.id_servico_jae_destino
                or m.id_servico_jae_destino is null
            )
            and (
                (
                    ifnull(m.modo_origem, '') in unnest(i.modos_origem)
                    and i.integracao_origem_regex is null
                )
                or regexp_extract(m.integracao_origem, i.integracao_origem_regex)
                is not null
            )
    ),
    integracao_matriz_union_bum_buc as (
        select *
        from integracao_buc_matriz

        union all by name

        select *
        from integracao_bum_matriz
    ),
    integracao_array_tratado as (
        /*
        Faz o tratamento do array gerado anteriormente em uma tabela auxiliar
        */
        select
            id_integracao,
            array_agg(distinct data_transacao) as datas_transacoes,
            array_agg(a.percentual_rateio order by rn, a.ordem) as rateio_realizado,
            concat(
                "^", string_agg(a.modo, '-' order by rn, a.ordem), "$"
            ) as integracao_realizada_regex
        from integracao_matriz_union_bum_buc, unnest(array_integracao) as a
        group by 1
    ),
    integracao_reparticao as (
        select
            im.data,
            im.id_integracao,
            iat.integracao_realizada_regex,
            im.indicador_integracao_fora_matriz,
            im.tempo_integracao_minutos as tempo_integracao_minutos_matriz,
            im.datetime_transacao_destino,
            im.datetime_transacao_origem,
            iat.rateio_realizado,
            array(
                select round(s, 2) from unnest(mrt.sequencia_rateio) s
            ) as rateio_matriz,
            iat.datas_transacoes,
            "Integração" as tipo_integracao
        from integracao_matriz_union_bum_buc im
        join integracao_array_tratado iat using (id_integracao)
        left join
            {{ ref("matriz_reparticao_tarifaria") }} mrt
            on mrt.data_inicio_matriz <= im.data_lead
            and (mrt.data_fim_matriz >= im.data_lead or mrt.data_fim_matriz is null)
            and regexp_extract(mrt.integracao, iat.integracao_realizada_regex)
            is not null
        qualify
            row_number() over (
                partition by id_integracao, sequencia_integracao
                order by mrt.integracao like "% %" desc
            )
            = 1
    ),
    integracao_agregada as (
        /*
        1. Agrega cada integração em uma linha
        2. Pega informações de rateio da matriz de repartição
        */
        select
            data,
            id_integracao,
            integracao_realizada_regex,
            max(indicador_integracao_fora_matriz) as indicador_integracao_fora_matriz,
            tempo_integracao_minutos_matriz,
            sum(
                datetime_diff(
                    datetime_transacao_destino, datetime_transacao_origem, minute
                )
            ) as tempo_integracao_minutos_realizado,
            rateio_realizado,
            rateio_matriz,
            datas_transacoes,
            tipo_integracao
        from integracao_reparticao
        group by all
    ),
    transferencia_buc as (
        /*
        Filtra somente as transferências
        */
        select
            * except (rn),
            row_number() over (
                partition by id_integracao order by sequencia_integracao
            ) as rn,
            "BUC" as tipo_bilhete_unico
        from integracao_buc
        where
            (
                exists (select 1 from unnest(modos_origem) m where m in ('BRT', 'VLT'))
                and exists (
                    select 1
                    from unnest(modos_origem) o
                    join unnest(modos_destino) d on o = d
                )
            )
    ),
    transferencia_bum as (
        select
            i.* except (rn),
            row_number() over (
                partition by id_integracao order by i.sequencia_integracao
            ) as rn,
            "BUM" as tipo_bilhete_unico
        from integracao_origem_destino_tratado i
        left join integracao_bum_matriz b using (id_integracao)
        where
            'VLT' in unnest(i.modos_origem)
            and 'VLT' in unnest(i.modos_destino)
            and b.id_integracao is not null
    ),
    union_transferencia_bum_buc as (
        select *
        from transferencia_buc

        union all

        select *
        from transferencia_bum
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
        from union_transferencia_bum_buc
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
                        struct(t.modos_origem_string as modo, 1 as ordem),
                        struct(t.modos_destino_string as modo, 2 as ordem)
                    ]
                else [struct(t.modos_destino_string as modo, 1 as ordem)]  -- transações seguintes somente destino (a origem é igual ao destino da transação anterior)
            end as array_modo
        from transferencia_id t
        left join
            matriz_integracao m
            on m.tipo_integracao != "Transferência"
            and t.tipo_bilhete_unico = m.tipo_bilhete_unico
            and (t.data_lead <= m.data_fim or m.data_fim is null)
            and m.modo_origem in unnest(t.modos_origem)
            and (
                t.id_servico_jae_origem = m.id_servico_jae_origem
                or m.id_servico_jae_origem is null
            )
            and m.modo_destino in unnest(t.modos_destino)
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
            concat(
                "^", string_agg(a.modo, '-' order by rn, a.ordem), "$"
            ) as transferencia_realizada_regex
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
            tat.transferencia_realizada_regex,
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
                    transferencia_realizada_regex as transferencia_realizada_regex,
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
            i.integracao_realizada_regex,
            i.tempo_integracao_minutos_realizado,
            i.tempo_integracao_minutos_matriz,
            i.rateio_realizado,
            i.rateio_matriz,
            t.transferencias_realizadas,
            i.indicador_integracao_fora_matriz,
            i.tempo_integracao_minutos_matriz
            < i.tempo_integracao_minutos_realizado
            as indicador_tempo_integracao_invalido,
            if(
                not i.indicador_integracao_fora_matriz,
                to_json_string(i.rateio_realizado) != to_json_string(i.rateio_matriz),
                null
            ) as indicador_rateio_invalido,
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
