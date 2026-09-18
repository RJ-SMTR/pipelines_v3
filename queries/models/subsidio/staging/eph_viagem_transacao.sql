{{ config(materialized="ephemeral") }}

{% if var("flow_name") == "treatment--monitoramento-temperatura" %}
    {% set interval_minutes = 120 %}
{% else %} {% set interval_minutes = 30 %}
{% endif %}

{% set date_range_start %}
  {% if var("flow_name") == "treatment--monitoramento-temperatura" or var("sistema") == "rio" %}
        "{{ var('date_range_start') }}"
    {% else %}
       "{{ var('start_date') }}"
    {% endif %}
{% endset %}
{% set date_range_end %}
  {% if var("flow_name") == "treatment--monitoramento-temperatura" or var("sistema") == "rio" %}
        "{{ var('date_range_end') }}"
    {% else %}
       "{{ var('end_date') }}"
    {% endif %}
{% endset %}

with
    -- Transações Jaé
    transacao as (
        select t.id_veiculo, t.servico_jae, t.datetime_transacao
        from {{ ref("transacao") }} as t
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`
        where
            t.data between date({{ date_range_start }}) and date_add(
                date({{ date_range_end }}), interval 1 day
            )
            and date(t.datetime_processamento) - date(t.datetime_transacao)
            <= interval 6 day
            and t.modo = "Ônibus"
    ),

    -- Transações RioCard
    transacao_riocard as (
        select t.id_veiculo, t.servico_jae, t.datetime_transacao
        from {{ ref("transacao_riocard") }} as t
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard`
        where
            t.data between date({{ date_range_start }}) and date_add(
                date({{ date_range_end }}), interval 1 day
            )
            and date(t.datetime_processamento) - date(t.datetime_transacao)
            <= interval 6 day
            and t.modo = "Ônibus"
    ),

    -- -- Viagens realizadas
    viagem as (
        select
            data,
            id_viagem,
            id_veiculo,
            -- Chave de join com a Jaé, que guarda o `id_veiculo` sem o prefixo de
            -- lote (RIO, `{lote}-{prefixo}`) ou sem o primeiro dígito (SPPO). Tem
            -- que ser expressão de uma só tabela, senão o BigQuery não extrai chave
            -- de hash e o join com a transação vira produto cartesiano.
            {% if var("sistema") == "rio" %}
                if(
                    substr(id_veiculo, 3, 1) = "-", substr(id_veiculo, 4), null
                ) as id_veiculo_join,
            {% else %} substr(id_veiculo, 2) as id_veiculo_join,
            {% endif %}
            {% if var("sistema") == "rio" %} id_validador,
            {% else %} cast(null as string) as id_validador,
            {% endif %}
            datetime_partida,
            datetime_chegada,
            modo,
            tecnologia_apurada,
            tecnologia_remunerada,
            {% if var("sistema") == "rio" %} cast(null as string) as tipo_viagem,
            {% else %} tipo_viagem,
            {% endif %}
            indicadores,
            servico,
            sentido,
            distancia_planejada
        {% if var("sistema") == "rio" %} from {{ ref("viagem_valida_temperatura") }}
        {% else %} from {{ ref("viagem_regularidade_temperatura") }}
        {% endif %}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
            and (
                data between date({{ date_range_start }}) and date({{ date_range_end }})
                {% if target.name == "prod" %}
                    or data = date_sub(date({{ date_range_start }}), interval 1 day)
                {% endif %}
            )

        {% if target.name in ("dev", "hmg") %}
            --fmt:off
            left outer union all by name
             --fmt:on
            (
                select id_veiculo, datetime_partida, datetime_chegada
                from `rj-smtr.projeto_subsidio_sppo.viagem_completa`
                where
                    data = date_sub(date({{ date_range_start }}), interval 1 day)
                    and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
                    and data < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")

                union all by name

                select id_veiculo, datetime_partida, datetime_chegada
                from `rj-smtr.monitoramento.viagem_valida`
                where
                    data = date_sub(date({{ date_range_start }}), interval 1 day)
                    and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
            )
        {% endif %}
    ),

    -- Viagem, para fins de contagem de passageiros, com tolerância de 30 minutos,
    -- limitada pela viagem anterior
    viagem_com_tolerancia_previa as (
        select
            v.*,
            lag(v.datetime_chegada) over (
                partition by v.id_veiculo order by v.datetime_partida
            ) as viagem_anterior_chegada,
            case
                when
                    lag(v.datetime_chegada) over (
                        partition by v.id_veiculo order by v.datetime_partida
                    )
                    is null
                then
                    datetime(
                        timestamp_sub(
                            datetime_partida, interval {{ interval_minutes }} minute
                        )
                    )
                else
                    datetime(
                        timestamp_add(
                            greatest(
                                timestamp_sub(
                                    datetime_partida,
                                    interval {{ interval_minutes }} minute
                                ),
                                lag(v.datetime_chegada) over (
                                    partition by v.id_veiculo
                                    order by v.datetime_partida
                                )
                            ),
                            interval 1 second
                        )
                    )
            end as datetime_partida_com_tolerancia
        from viagem as v
    ),

    -- Considera apenas as viagens realizadas no período de apuração
    viagem_com_tolerancia as (
        select *
        from viagem_com_tolerancia_previa
        where data between date({{ date_range_start }}) and date({{ date_range_end }})
    ),

    -- Contagem de transações Jaé
    transacao_contagem as (
        select
            v.data,
            v.id_viagem,
            count(t.datetime_transacao) as quantidade_transacao,
            countif(
                v.servico != t.servico_jae and t.datetime_transacao > v.datetime_partida
            ) as quantidade_transacao_servico_divergente
        from transacao as t
        inner join
            viagem_com_tolerancia as v
            on t.id_veiculo = v.id_veiculo_join
            and t.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by 1, 2
    ),

    -- Contagem de transações RioCard
    transacao_riocard_contagem as (
        select
            v.data,
            v.id_viagem,
            count(tr.datetime_transacao) as quantidade_transacao_riocard,
            countif(
                v.servico != tr.servico_jae
                and tr.datetime_transacao > v.datetime_partida
            ) as quantidade_transacao_riocard_servico_divergente
        from transacao_riocard as tr
        inner join
            viagem_com_tolerancia as v
            on tr.id_veiculo = v.id_veiculo_join
            and tr.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by 1, 2
    ),

    -- Calcula a porcentagem de estado do equipamento "ABERTO" por
    -- validador e
    -- viagem
    estado_equipamento_perc as (
        select
            v.data,
            v.id_viagem,
            coalesce(
                safe_cast(json_value(item, "$.id_validador") as string), v.id_validador
            ) as id_validador,
            coalesce(t.quantidade_transacao, 0) as quantidade_transacao,
            coalesce(
                tr.quantidade_transacao_riocard, 0
            ) as quantidade_transacao_riocard,
            coalesce(
                t.quantidade_transacao_servico_divergente, 0
            ) as quantidade_transacao_servico_divergente,
            coalesce(
                tr.quantidade_transacao_riocard_servico_divergente, 0
            ) as quantidade_transacao_riocard_servico_divergente,
            safe_cast(
                json_value(item, '$.percentual_estado_equipamento_aberto') as numeric
            ) as percentual_estado_equipamento_aberto,
            safe_cast(
                json_value(item, '$.indicador_estado_equipamento_aberto') as bool
            ) as indicador_estado_equipamento_aberto,
            safe_cast(
                json_value(item, '$.indicador_gps_servico_divergente') as bool
            ) as indicador_gps_servico_divergente
        from viagem as v
        left join
            transacao_contagem as t on v.data = t.data and v.id_viagem = t.id_viagem
        left join
            transacao_riocard_contagem as tr
            on v.data = tr.data
            and v.id_viagem = tr.id_viagem
        left join
            unnest(
                json_query_array(v.indicadores, '$.indicador_validador.valores')
            ) as item
    ),

    validador_tipo_viagem as (
        select
            data,
            id_viagem,
            id_validador,
            case
                when data < date('{{ var("DATA_SUBSIDIO_V12_INICIO") }}')
                then quantidade_transacao_riocard = 0
                else (quantidade_transacao_riocard = 0 and quantidade_transacao = 0)
            end as indicador_sem_transacao,
            indicador_estado_equipamento_aberto,
            (
                data >= date('{{ var("DATA_SUBSIDIO_V8_INICIO") }}')
                and (
                    (
                        data < date('{{ var("DATA_SUBSIDIO_V12_INICIO") }}')
                        and (
                            quantidade_transacao_riocard = 0
                            or not indicador_estado_equipamento_aberto
                        )
                    )
                    or (
                        data >= date('{{ var("DATA_SUBSIDIO_V12_INICIO") }}')
                        and data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                        and (
                            (
                                quantidade_transacao_riocard = 0
                                and quantidade_transacao = 0
                            )
                            or not indicador_estado_equipamento_aberto
                        )
                    )
                    or (
                        data >= date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                        and (
                            quantidade_transacao_riocard = 0
                            and quantidade_transacao = 0
                        )
                    )
                )
            ) as indicador_sem_transacao_tipo,
            (
                data >= date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                and not indicador_estado_equipamento_aberto
            ) as indicador_validador_fechado,
            (
                data >= date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                and (
                    quantidade_transacao_riocard_servico_divergente > 0
                    or quantidade_transacao_servico_divergente > 0
                    or indicador_gps_servico_divergente
                )
            ) as indicador_validador_associado_incorretamente
        from estado_equipamento_perc
    ),

    flags_viagem as (
        select
            data,
            id_viagem,
            max(indicador_sem_transacao) as indicador_sem_transacao,
            max(indicador_sem_transacao_tipo) as indicador_sem_transacao_tipo,
            logical_or(indicador_validador_fechado) as indicador_validador_fechado,
            logical_or(
                indicador_validador_associado_incorretamente
            ) as indicador_validador_associado_incorretamente,
            case
                when data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                then max(indicador_estado_equipamento_aberto)
                else min(indicador_estado_equipamento_aberto)
            end as indicador_estado_equipamento_aberto,
            array_agg(distinct id_validador ignore nulls) as id_validador
        from validador_tipo_viagem
        group by 1, 2
    )

select
    v.data,
    v.id_viagem,
    v.id_veiculo,
    v.servico,
    any_value(f.id_validador) as id_validador,
    v.tipo_viagem,
    f.indicador_sem_transacao,
    f.indicador_sem_transacao_tipo,
    f.indicador_validador_fechado,
    f.indicador_validador_associado_incorretamente,
    v.modo,
    v.tecnologia_apurada,
    v.tecnologia_remunerada,
    v.sentido,
    v.distancia_planejada,
    any_value(v.indicadores) as indicadores,
    any_value(eep.quantidade_transacao) as quantidade_transacao,
    any_value(eep.quantidade_transacao_riocard) as quantidade_transacao_riocard,
    case
        when v.data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
        then max(eep.percentual_estado_equipamento_aberto)
        else min(eep.percentual_estado_equipamento_aberto)
    end as percentual_estado_equipamento_aberto,
    f.indicador_estado_equipamento_aberto,
    v.datetime_partida_com_tolerancia as datetime_partida_bilhetagem,
    v.datetime_partida,
    v.datetime_chegada
from viagem_com_tolerancia as v
left join
    estado_equipamento_perc as eep on v.data = eep.data and v.id_viagem = eep.id_viagem
left join flags_viagem as f on v.data = f.data and v.id_viagem = f.id_viagem
group by all
