{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        alias="viagem_planejada",
        incremental_strategy="insert_overwrite",
    )
}}

-- depends_on: {{ ref('feed_info_gtfs') }}
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
                    {% if c == "trajetos_alternativos" %}ifnull(to_json_string({{ c }}), 'n/a')
                    {% else %}ifnull(cast({{ c }} as string), 'n/a')
                    {% endif %}
                    {% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        )
    {% endset %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
    {% set feed_filter %}
        feed_start_date in (
            date('{{ last_feed_version }}'), date('{{ var("data_versao_gtfs") }}')
        )
    {% endset %}
{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
    {% set feed_filter %} 1 = 0 {% endset %}
{% endif %}

with
    /*
    Trips do GTFS já tratadas, filtradas a partir do feed inicial
    configurado e, em execução incremental, restritas aos feeds afetados.
    */
    trips as (
        select *
        from {{ ref("aux_trips") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
    ),
    /*
    Frequencies do GTFS com horários já tratados (janelas de operação e
    headway por trip), usadas para gerar viagens baseadas em frequência.
    */
    frequencies_tratada as (
        select *
        from {{ ref("aux_frequencies_horario_tratado") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
    ),
    /*
    Cruza cada trip com sua frequência, anexando início, fim e headway da
    janela de operação. Só restam trips que possuem frequência definida.
    */
    trips_frequencies as (
        select t.*, f.start_seconds, f.end_seconds, f.headway_secs
        from trips t
        join frequencies_tratada f using (feed_start_date, trip_id)
    ),
    /*
    Trajetos alternativos por serviço/sentido/evento, com a extensão de cada alternativa.
    */
    trajeto_alternativo_sentido as (
        select *
        from {{ ref("aux_trajeto_alternativo_sentido") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
    ),
    /*
    Materializa as viagens das trips baseadas em frequência: expande cada
    janela em horários de partida discretos (start..end-1 a cada headway).
    */
    viagens_frequencies as (
        select
            tf.trip_id,
            tf.modo,
            tf.route_id,
            tf.service_id,
            tf.servico,
            tf.direction_id,
            tf.shape_id,
            tf.feed_version,
            tf.feed_start_date,
            tf.evento,
            partida_seconds
        from
            trips_frequencies tf,
            unnest(
                generate_array(tf.start_seconds, tf.end_seconds - 1, tf.headway_secs)
            ) as partida_seconds
    ),
    /*
    Viagens das trips SEM frequência: usa o horário de partida do primeiro
    ponto (stop_sequence = 0) do stop_times. O left join + f.trip_id is null
    garante que só entram trips ausentes de frequencies_tratada.
    */
    viagens_stop_times as (
        select
            t.trip_id,
            t.modo,
            t.route_id,
            t.service_id,
            t.servico,
            t.direction_id,
            t.shape_id,
            t.feed_version,
            t.feed_start_date,
            t.evento,
            st.arrival_seconds as partida_seconds
        from trips t
        join
            {{ ref("aux_stop_times_horario_tratado") }} st using (
                feed_start_date, trip_id
            )
        left join frequencies_tratada f using (feed_start_date, trip_id)
        where st.stop_sequence = 0 and f.trip_id is null
    ),
    /*
    Une os dois tipos de viagem (frequência + stop_times) num só conjunto,
    descartando o service_id 'EXCEP' (trajetos alternativos).
    */
    viagens_unidas as (
        select *
        from viagens_frequencies
        where service_id != 'EXCEP'
        union all
        select *
        from viagens_stop_times
        where service_id != 'EXCEP'
    ),
    /*
    Ordem de Serviço diária: extensão planejada por serviço, sentido, tipo de
    dia e tipo de OS. Fonte do tipo_dia e da extensão da viagem.
    */
    ordem_servico_extensao as (
        select
            feed_start_date, feed_version, servico, tipo_os, tipo_dia, sentido, extensao
        from {{ ref("aux_ordem_servico_diaria") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
    ),
    /*
    Identifica os shapes circulares (ponto inicial ≈ ponto final, via macro
    is_shape_circular). Usado para classificar o sentido como "C".
    */
    servico_circular as (
        select feed_start_date, shape_id
        from {{ ref("shapes_geom_planejamento") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
            and
            {{ is_shape_circular("start_pt", "end_pt", "feed_start_date", "shape_id") }}
    ),
    /*
    Monta a base da viagem planejada: converte partida_seconds em horário
    HH:MM:SS, deriva tipo_dia do service_id (Útil/Sábado/Domingo) e o sentido
    (C = circular, I = ida quando direction_id 0, V = volta caso contrário).
    */
    viagem_planejada_base as (
        select
            concat(
                lpad(cast(div(partida_seconds, 3600) as string), 2, '0'),
                ':',
                lpad(cast(mod(div(partida_seconds, 60), 60) as string), 2, '0'),
                ':',
                lpad(cast(mod(partida_seconds, 60) as string), 2, '0')
            ) as horario_partida,
            modo,
            service_id,
            case
                when service_id like "%U_%"
                then "Dia Útil"
                when service_id like "%S_%"
                then "Sabado"
                when service_id like "%D_%"
                then "Domingo"
                else service_id
            end as tipo_dia,
            trip_id,
            route_id,
            shape_id,
            servico,
            direction_id,
            case
                when c.shape_id is not null
                then "C"
                when direction_id = '0'
                then "I"
                else "V"
            end as sentido,
            evento,
            feed_version,
            feed_start_date
        from viagens_unidas v
        left join servico_circular c using (shape_id, feed_start_date)
    ),
    /*
    Enriquece a base com dados da Ordem de Serviço (tipo_os e extensão),
    unindo por feed/serviço/sentido. O tipo_dia é resolvido pela
    macro ordem_servico_excecoes_join, que trata o caso padrão (mesmo tipo_dia)
    e as exceções de calendário (Ponto Facultativo, ENEM, Verão, Réveillon
    etc.). O tipo_dia da OS prevalece quando há correspondência.
    */
    viagem_planejada_os as (
        select
            v.* except (tipo_dia),
            coalesce(ose.tipo_dia, v.tipo_dia) as tipo_dia,
            ose.tipo_os,
            ose.extensao
        from viagem_planejada_base v
        left join
            ordem_servico_extensao ose
            on ose.feed_start_date = v.feed_start_date
            and ose.servico = v.servico
            and ose.sentido = v.sentido
            and {{ ordem_servico_excecoes_join("ose", "v") }}
    ),
    /*
    Constrói o array de trajetos alternativos (trip, shape, evento e extensão)
    por serviço/sentido/tipo_os, ignorando alternativas sem extensão válida.
    */
    trips_alternativas as (
        select
            t.feed_start_date,
            t.feed_version,
            t.servico,
            t.direction_id,
            tas.tipo_os,
            array_agg(
                case
                    when tas.extensao is not null and tas.extensao != 0
                    then
                        struct(
                            t.trip_id as trip_id,
                            t.shape_id as shape_id,
                            t.evento as evento,
                            tas.extensao as extensao
                        )
                end ignore nulls
            ) as trajetos_alternativos
        from trips t
        left join
            trajeto_alternativo_sentido tas
            on t.feed_start_date = tas.feed_start_date
            and t.servico = tas.servico
            and t.evento = tas.evento
            and t.direction_id = tas.direction_id
        where t.trip_id not in (select trip_id from frequencies_tratada)
        group by 1, 2, 3, 4, 5
    ),
    /*
    Une o array de trajetos alternativos a cada viagem planejada.
    */
    viagem_planejada_completa as (
        select v.*, ta.trajetos_alternativos
        from viagem_planejada_os v
        left join
            trips_alternativas ta using (
                feed_start_date, servico, direction_id, tipo_os
            )
    ),
    /*
    Constrói o identificador único da viagem (id_viagem) concatenando
    serviço, sentido, shape, service_id, tipo_os, tipo_dia normalizado e
    horário de partida.
    */
    viagem_planejada_id as (
        select
            concat(
                servico,
                "_",
                sentido,
                "_",
                shape_id,
                "_",
                service_id,
                "_",
                coalesce(tipo_os, 'NA'),
                "_",
                {{ normalize_text("tipo_dia", snake_case=true) }},
                "_",
                replace(horario_partida, ':', '')
            ) as id_viagem,
            * except (direction_id)
        from viagem_planejada_completa
    ),
    /*
    Dados novos para materialização.
    */
    dados_novos as (select * from viagem_planejada_id),
    /*
    Em execução incremental, lê os registros já existentes na tabela para os
    feeds afetados, base para comparação de mudanças.
    */
    {% if is_incremental() %}
        dados_atuais as (select * from {{ this }} where {{ feed_filter }}),
    {% endif %}
    /*
    Calcula o hash (sha256) de cada registro atual e guarda os metadados de
    controle (datetime de atualização e id de execução). No full-refresh
    devolve nulos para que tudo seja tratado como dado novo.
    */
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                id_viagem,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais
        {% else %}
            select
                cast(null as string) as id_viagem,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    /*
    Junta cada dado novo com seu hash e o hash do dado atual correspondente,
    para depois detectar se houve mudança de conteúdo.
    */
    sha_dados_completos as (
        select n.*, {{ sha_column }} as sha_dado_novo, a.* except (id_viagem)
        from dados_novos n
        left join sha_dados_atuais a using (id_viagem)
    ),
    /*
    Define as colunas de controle: versão, datetime_ultima_atualizacao e
    id_execucao_dbt só são renovados quando o hash mudou (ou é registro
    novo); caso contrário preserva os valores atuais.
    */
    colunas_controle as (
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
from colunas_controle
