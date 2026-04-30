{% if var("tipo_materializacao") == "monitoramento" %}
    {{
        config(
            partition_by={
                "field": "data",
                "data_type": "date",
                "granularity": "day",
            },
            schema="monitoramento_interno",
        )
    }}
{% else %}
    {{
        config(
            partition_by={
                "field": "data",
                "data_type": "date",
                "granularity": "day",
            },
        )
    }}
{% endif %}

{% set incremental_filter %}
    data between
        date_sub(date('{{ var("date_range_start") }}'), interval 1 day)
        and date('{{ var("date_range_end") }}')
{% endset %}

{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
{% if execute %}
    {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
        {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
        {% endset %}
        {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    /*
    Agregação para cálculo da quantidade de segmentos verificados, válidos e tolerados por viagem
    */
    contagem as (
        select
            data,
            id_viagem,
            any_value(id_viagem_planejada) as id_viagem_planejada,
            any_value(datetime_partida_informada) as datetime_partida_informada,
            any_value(datetime_chegada_informada) as datetime_chegada_informada,
            any_value(datetime_partida_automatica) as datetime_partida_automatica,
            any_value(datetime_chegada_automatica) as datetime_chegada_automatica,
            any_value(datetime_partida_considerada) as datetime_partida_considerada,
            any_value(datetime_chegada_considerada) as datetime_chegada_considerada,
            any_value(modo) as modo,
            any_value(id_veiculo) as id_veiculo,
            any_value(trip_id) as trip_id,
            any_value(route_id) as route_id,
            any_value(shape_id) as shape_id,
            any_value(servico) as servico,
            any_value(sentido) as sentido,
            countif(id_segmento is not null) as quantidade_segmentos_considerados,
            countif(quantidade_gps > 0) as quantidade_segmentos_validos,
            round(
                countif(id_segmento is not null)
                * safe_cast((1 - {{ var("parametro_validacao") }}) as numeric)
            ) as quantidade_segmentos_tolerados,
            logical_or(
                indicador_primeiro_segmento_valido
            ) as indicador_primeiro_segmento_valido,
            logical_or(
                indicador_ultimo_segmento_valido
            ) as indicador_ultimo_segmento_valido,
            max(indicador_servico_divergente) as indicador_servico_divergente,
            max(id_segmento is null) as indicador_shape_invalido,
            any_value(service_ids) as service_ids,
            any_value(tipo_dia) as tipo_dia,
            any_value(feed_version) as feed_version,
            any_value(feed_start_date) as feed_start_date,
            any_value(datetime_processamento) as datetime_processamento,
            any_value(datetime_captura_viagem) as datetime_captura_viagem
        from {{ ref("gps_segmento_viagem") }}
        {# from `rj-smtr`.`monitoramento_staging`.`gps_segmento_viagem` #}
        where
            (
                not indicador_segmento_desconsiderado
                or indicador_segmento_desconsiderado is null
            )
            {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
                and {{ incremental_filter }}
            {% endif %}
        group by data, id_viagem
    ),
    /*
    Calcula o índice de validação, quantidade mínima de segmentos necessários e indicador de campos obrigatórios por viagem
    */
    indice as (
        select
            data,
            id_viagem,
            id_viagem_planejada,
            datetime_partida_informada,
            datetime_chegada_informada,
            datetime_partida_automatica,
            datetime_chegada_automatica,
            datetime_partida_considerada,
            datetime_chegada_considerada,
            modo,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            quantidade_segmentos_considerados,
            quantidade_segmentos_validos,
            case
                when quantidade_segmentos_tolerados >= 1
                then quantidade_segmentos_considerados - quantidade_segmentos_tolerados
                else abs(quantidade_segmentos_considerados - 1)
            end as quantidade_segmentos_necessarios,
            safe_divide(
                quantidade_segmentos_validos, quantidade_segmentos_considerados
            ) as indice_validacao,
            indicador_primeiro_segmento_valido,
            indicador_ultimo_segmento_valido,
            indicador_servico_divergente,
            indicador_shape_invalido,
            (
                id_viagem is not null
                and datetime_partida_considerada is not null
                and datetime_chegada_considerada is not null
                and shape_id is not null
                and route_id is not null
                and id_veiculo is not null
            ) as indicador_campos_obrigatorios,
            datetime_chegada_considerada
            > datetime_partida_considerada as indicador_chegada_posterior_partida,
            service_ids,
            tipo_dia,
            feed_version,
            feed_start_date,
            datetime_processamento,
            datetime_captura_viagem
        from contagem
    ),
    /*
    Filtra apenas viagens com todos os campos obrigatórios preenchidos
    */
    viagens_campos_obrigatorios as (
        select * from indice where indicador_campos_obrigatorios
    ),
    /*
    Agregação dos service_ids por feed_start_date, feed_version e route_id
    */
    trips as (
        select distinct
            feed_start_date,
            feed_version,
            route_id,
            array_agg(service_id) as service_ids,
        from {{ ref("trips_gtfs") }}
        {# from `rj-smtr.gtfs.trips` #}
        {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
        group by 1, 2, 3
    ),
    /*
    Verifica se o serviço da viagem está planejado no GTFS
    */
    servicos_planejados_gtfs as (
        select
            v.*,
            (
                select count(*)
                from unnest(v.service_ids) as service_id
                join unnest(t.service_ids) as service_id using (service_id)
            )
            > 0 as indicador_servico_planejado_gtfs
        from viagens_campos_obrigatorios v
        left join trips t using (feed_start_date, feed_version, route_id)
    ),
    /*
    Quilometragem planejada por serviço, faixa horária e sentido
    */
    servico_planejado as (
        select
            data,
            servico,
            sentido,
            extensao,
            quilometragem,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            trip_info
        from {{ ref("servico_planejado_faixa_horaria") }}
        {# from `rj-smtr`.`planejamento`.`servico_planejado_faixa_horaria` #}
        {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
            where {{ incremental_filter }}
        {% endif %}
    ),
    /*
    Desaninhamento da informação do shape_id e indicador_trajeto_alternativo do array trip_info
    */
    servico_planejado_unnested as (
        select distinct
            sp.data,
            sp.servico,
            sp.sentido,
            sp.extensao,
            sp.quilometragem,
            sp.faixa_horaria_inicio,
            sp.faixa_horaria_fim,
            trip.shape_id,
            trip.indicador_trajeto_alternativo
        from servico_planejado sp, unnest(sp.trip_info) as trip
        where trip.shape_id is not null
    ),
    /*
    Verifica se o serviço está planejado na OS e calcula a velocidade média da viagem
    */
    servicos_planejados_os as (
        select
            spg.*,
            spu.extensao as distancia_planejada,
            spu.indicador_trajeto_alternativo,
            -- fmt: off
            safe_divide(spu.extensao*3600, datetime_diff(datetime_chegada_considerada, datetime_partida_considerada, second)) as velocidade_media,
            -- fmt: on
            case
                when spu.quilometragem is not null and spu.quilometragem > 0
                then true
                when
                    (spu.quilometragem is not null and spu.quilometragem <= 0)
                    or (spu.quilometragem is null and spg.modo = "Ônibus SPPO")
                then false
            end as indicador_servico_planejado_os
        from servicos_planejados_gtfs spg
        left join
            servico_planejado_unnested spu
            on spu.servico = spg.servico
            and spu.data = spg.data
            and spu.shape_id = spg.shape_id
            and spg.datetime_partida_considerada
            between spu.faixa_horaria_inicio and spu.faixa_horaria_fim
    ),
    /*
    Verifica se a velocidade média da viagem está acima do limite máximo permitido
    */
    viagens_velocidade_media as (
        select
            *,
            velocidade_media
            >= {{ var("conformidade_velocidade_min") }}
            as indicador_acima_velocidade_max
        from servicos_planejados_os
    ),
    /*
    Verifica se a viagem está sobreposta a outra viagem do mesmo veículo
    */
    viagens_sobrepostas as (
        select
            v1.data,
            v1.id_viagem,
            v1.id_veiculo,
            v2.id_viagem as id_viagem_sobreposta,
            v2.datetime_captura_viagem as datetime_captura_viagem_sobreposta,
            case
                when v2.id_viagem is not null
                then
                    case
                        when v1.datetime_captura_viagem = v2.datetime_captura_viagem
                        then true
                        when v1.datetime_captura_viagem < v2.datetime_captura_viagem
                        then true
                        else false
                    end
                else false
            end as indicador_viagem_sobreposta
        from viagens_velocidade_media v1
        left join
            viagens_velocidade_media v2
            on (
                v1.data between date_sub(v2.data, interval 1 day) and date_add(
                    v2.data, interval 1 day
                )
            )
            and v1.id_veiculo = v2.id_veiculo
            and v1.id_viagem != v2.id_viagem
            and v1.datetime_partida_considerada < v2.datetime_chegada_considerada
            and v1.datetime_chegada_considerada > v2.datetime_partida_considerada
        qualify
            row_number() over (
                partition by v1.id_viagem
                order by
                    v2.datetime_captura_viagem desc, v2.datetime_partida_considerada
            )
            = 1
    ),
    /*
    Indicadores de processamento e prazo de envio da viagem informada
    */
    viagem_informada_indicadores as (
        select
            id_viagem,
            ifnull(
                datetime_processamento > datetime_captura_viagem, false
            ) as indicador_processamento_posterior_captura,
            ifnull(
                datetime_processamento < datetime_chegada_considerada, false
            ) as indicador_processamento_anterior_chegada,
            ifnull(
                datetime_processamento is not null
                and (
                    select countif(tipo_dia = 'Dia Útil')
                    from {{ calendario }}
                    where data > v.data and data <= date(v.datetime_processamento)
                )
                <= 2,
                false
            ) as indicador_prazo_envio
        from viagens_velocidade_media v
    ),
    /*
    Agregação dos indicadores de validação da viagem
    */
    viagens as (
        select
            vm.data,
            vm.id_viagem,
            vm.id_viagem_planejada,
            vm.datetime_partida_informada,
            vm.datetime_chegada_informada,
            vm.datetime_partida_automatica,
            vm.datetime_chegada_automatica,
            vm.datetime_partida_considerada,
            vm.datetime_chegada_considerada,
            vm.modo,
            vm.id_veiculo,
            vm.trip_id,
            vm.route_id,
            vm.shape_id,
            vm.servico,
            vm.sentido,
            vm.distancia_planejada,
            vm.velocidade_media,
            vm.quantidade_segmentos_considerados,
            vm.quantidade_segmentos_validos,
            vm.quantidade_segmentos_necessarios,
            vm.indice_validacao,
            vs.indicador_viagem_sobreposta,
            -- fmt: off
            vm.quantidade_segmentos_validos >= vm.quantidade_segmentos_necessarios as indicador_trajeto_valido,
            -- fmt: on
            vm.indicador_servico_planejado_gtfs,
            vm.indicador_servico_planejado_os,
            vm.indicador_primeiro_segmento_valido,
            vm.indicador_ultimo_segmento_valido,
            vm.indicador_servico_divergente,
            vm.indicador_shape_invalido,
            vm.indicador_campos_obrigatorios,
            vm.indicador_chegada_posterior_partida,
            vm.indicador_trajeto_alternativo,
            vm.indicador_acima_velocidade_max,
            vi.indicador_processamento_posterior_captura,
            vi.indicador_processamento_anterior_chegada,
            vi.indicador_prazo_envio,
            (
                vm.indicador_campos_obrigatorios
                and vm.indicador_chegada_posterior_partida
                and not vm.indicador_shape_invalido
                -- fmt: off
                and vm.quantidade_segmentos_validos >= vm.quantidade_segmentos_necessarios
                -- fmt: on
                and vm.indicador_servico_planejado_gtfs
                {% if var("tipo_materializacao") != "monitoramento" %}
                    and not vs.indicador_viagem_sobreposta
                {% endif %}
                and not vm.indicador_acima_velocidade_max
                and ifnull(vm.indicador_primeiro_segmento_valido, false)
                and ifnull(vm.indicador_ultimo_segmento_valido, false)
                and ifnull(vm.indicador_servico_planejado_os, true)
                and not vi.indicador_processamento_posterior_captura
                and not vi.indicador_processamento_anterior_chegada
                and vi.indicador_prazo_envio
            ) as indicador_viagem_valida,
            vm.tipo_dia,
            vm.feed_version,
            vm.feed_start_date
        from viagens_velocidade_media vm
        left join viagens_sobrepostas vs using (id_viagem)
        left join viagem_informada_indicadores vi using (id_viagem)
    ),
    -- fmt: off
    /*
    União entre viagens com indicadores de validação e viagens com campos obrigatórios incompletos
    */
    viagem_completa as (
        select *
        from viagens

        full outer union all by name

        select *, indicador_campos_obrigatorios as indicador_viagem_valida
        from indice
        where not indicador_campos_obrigatorios
    ),
    -- fmt: on
    /*
    Filtra viagens duplicadas considerando melhor índice de validação e maior distância
    */
    filtro_desvio as (
        select *
        from viagens
        qualify
            row_number() over (
                partition by
                    id_veiculo,
                    datetime_partida_considerada,
                    datetime_chegada_considerada
                order by
                    indice_validacao desc,
                    indicador_trajeto_alternativo,
                    distancia_planejada desc
            )
            = 1
    ),
    /*
    Filtra viagens duplicadas por horário de partida considerando maior distância planejada
    */
    filtro_partida as (
        select *
        from filtro_desvio
        qualify
            row_number() over (
                partition by id_veiculo, datetime_partida_considerada
                order by distancia_planejada desc
            )
            = 1
    ),
    /*
    Filtra viagens duplicadas por horário de chegada considerando maior distância planejada
    */
    filtro_chegada as (
        select *
        from filtro_partida
        qualify
            row_number() over (
                partition by id_veiculo, datetime_chegada_considerada
                order by distancia_planejada desc
            )
            = 1
    )
select
    data,
    id_viagem,
    id_viagem_planejada,
    datetime_partida_informada,
    datetime_chegada_informada,
    datetime_partida_automatica,
    datetime_chegada_automatica,
    datetime_partida_considerada,
    datetime_chegada_considerada,
    modo,
    id_veiculo,
    trip_id,
    route_id,
    shape_id,
    servico,
    sentido,
    distancia_planejada,
    velocidade_media,
    quantidade_segmentos_considerados,
    quantidade_segmentos_validos,
    quantidade_segmentos_necessarios,
    indice_validacao,
    indicador_viagem_sobreposta,
    indicador_trajeto_valido,
    indicador_servico_planejado_gtfs,
    indicador_servico_planejado_os,
    indicador_primeiro_segmento_valido,
    indicador_ultimo_segmento_valido,
    indicador_servico_divergente,
    indicador_shape_invalido,
    indicador_campos_obrigatorios,
    indicador_chegada_posterior_partida,
    indicador_trajeto_alternativo,
    indicador_acima_velocidade_max,
    indicador_processamento_posterior_captura,
    indicador_processamento_anterior_chegada,
    indicador_prazo_envio,
    indicador_viagem_valida,
    tipo_dia,
    feed_version,
    feed_start_date,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
{% if var("tipo_materializacao") == "monitoramento" %} from filtro_chegada
{% else %} from viagem_completa
{% endif %}
where
    data between date('{{ var("date_range_start") }}') and date(
        '{{ var("date_range_end") }}'
    )
