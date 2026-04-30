{{ config(materialized="ephemeral") }}

{% if var("tipo_materializacao") == "monitoramento" %} {% set interval_minutes = 120 %}
{% elif var("tipo_materializacao") == "subsidio" %} {% set interval_minutes = 30 %}
{% endif %}

{% set date_range_start %}
    {% if var("flow_name") == "monitoramento_temperatura - materializacao" %} 
        "{{var('date_range_start')}}" 
    {% else %}
       "{{var('start_date')}}"
    {% endif %}
{% endset %}
{% set date_range_end %}
    {% if var("flow_name") == "monitoramento_temperatura - materializacao" %}
        "{{var('date_range_end')}}" 
    {% else %}
       "{{var('end_date')}}"
    {% endif %}
{% endset %}

with
    -- Transações Jaé
    transacao as (
        select id_veiculo, servico_jae, datetime_transacao
        from {{ ref("transacao") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`
        where
            data between date({{ date_range_start }}) and date_add(
                date({{ date_range_end }}), interval 1 day
            )
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
            and modo = "Ônibus"
    ),
    -- Transações RioCard
    transacao_riocard as (
        select id_veiculo, servico_jae, datetime_transacao
        from {{ ref("transacao_riocard") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard`
        where
            data between date({{ date_range_start }}) and date_add(
                date({{ date_range_end }}), interval 1 day
            )
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
            and modo = "Ônibus"
    ),
    -- -- Viagens realizadas
    viagem as (
        select
            data,
            id_viagem,
            id_veiculo,
            datetime_partida,
            datetime_chegada,
            modo,
            tecnologia_apurada,
            tecnologia_remunerada,
            tipo_viagem,
            indicadores,
            servico,
            sentido,
            distancia_planejada,
        from {{ ref("viagem_regularidade_temperatura") }}
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
            select id_veiculo, datetime_partida, datetime_chegada
            from {{ ref("viagem_completa") }}
            where
                data = date_sub(date({{ date_range_start }}), interval 1 day)
                and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
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
        join
            viagem_com_tolerancia as v
            on t.id_veiculo = substr(v.id_veiculo, 2)
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
        join
            viagem_com_tolerancia as v
            on tr.id_veiculo = substr(v.id_veiculo, 2)
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
            safe_cast(json_value(item, '$.id_validador') as string) as id_validador,
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
        from viagem v
        left join transacao_contagem t on v.data = t.data and v.id_viagem = t.id_viagem
        left join
            transacao_riocard_contagem tr
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
            case
                when
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
                then 'Sem transação'

                when
                    data >= date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                    and not indicador_estado_equipamento_aberto
                then 'Validador fechado'

                when
                    data >= date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                    and (
                        quantidade_transacao_riocard_servico_divergente > 0
                        or quantidade_transacao_servico_divergente > 0
                        or indicador_gps_servico_divergente
                    )
                then 'Validador associado incorretamente'
                else 'Manter tipo viagem'
            end as tipo_viagem
        from estado_equipamento_perc
    ),
    prioridade_tipo_viagem as (
        select 'Sem transação' as tipo_viagem, 1 as prioridade
        union all
        select 'Validador fechado', 2
        union all
        select 'Validador associado incorretamente', 3
        union all
        select 'Manter tipo viagem', 4
    ),
    estado_equipamento as (
        select
            data,
            id_viagem,
            max(indicador_sem_transacao) as indicador_sem_transacao,
            case
                when data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                then max(indicador_estado_equipamento_aberto)
                else min(indicador_estado_equipamento_aberto)
            end as indicador_estado_equipamento_aberto
        from validador_tipo_viagem
        group by 1, 2
    ),
    validadores_agrupados as (
        select
            vtv.data,
            vtv.id_viagem,
            vtv.tipo_viagem,
            array_agg(distinct vtv.id_validador ignore nulls) as id_validador,
            ee.indicador_sem_transacao,
            ee.indicador_estado_equipamento_aberto,
            ptv.prioridade
        from validador_tipo_viagem vtv
        join prioridade_tipo_viagem ptv using (tipo_viagem)
        join estado_equipamento ee using (data, id_viagem)
        group by all
        qualify
            row_number() over (
                partition by vtv.data, vtv.id_viagem order by ptv.prioridade
            )
            = 1
    )
select
    v.data,
    v.id_viagem,
    v.id_veiculo,
    v.servico,
    va.id_validador,
    case
        when
            v.tipo_viagem not in (
                "Licenciado com ar e não autuado", "Licenciado sem ar e não autuado"
            )
            or va.tipo_viagem = "Manter tipo viagem"
        then v.tipo_viagem
        when
            v.data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
            and va.tipo_viagem = "Sem transação"
            and not va.indicador_sem_transacao
            and va.indicador_estado_equipamento_aberto
        then v.tipo_viagem
        else va.tipo_viagem
    end as tipo_viagem,
    v.modo,
    v.tecnologia_apurada,
    v.tecnologia_remunerada,
    v.sentido,
    v.distancia_planejada,
    any_value(eep.quantidade_transacao) as quantidade_transacao,
    any_value(eep.quantidade_transacao_riocard) as quantidade_transacao_riocard,
    case
        when v.data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
        then max(eep.percentual_estado_equipamento_aberto)
        else min(eep.percentual_estado_equipamento_aberto)
    end as percentual_estado_equipamento_aberto,
    va.indicador_estado_equipamento_aberto,
    v.datetime_partida_com_tolerancia as datetime_partida_bilhetagem,
    v.datetime_partida,
    v.datetime_chegada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_com_tolerancia as v
left join estado_equipamento_perc as eep using (data, id_viagem)
left join validadores_agrupados as va using (data, id_viagem)
group by all
