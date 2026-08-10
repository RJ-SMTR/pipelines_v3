{#
  Integridade no materializado: mesmo veículo não pode ter viagens sobrepostas
  na janela de partições (data entre start_date e end_date, incluindo dia
  anterior para cruzamento de meia-noite).
#}
{% test viagem_completa_sem_sobreposicao(model) %}
    with
        base as (
            select id_viagem, id_veiculo, data, datetime_partida, datetime_chegada
            from {{ model }}
            where
                data between date_sub(
                    date("{{ var('start_date') }}"), interval 1 day
                ) and date("{{ var('end_date') }}")
        )
    select
        v1.id_viagem as id_viagem_1,
        v2.id_viagem as id_viagem_2,
        v1.id_veiculo,
        v1.datetime_partida as partida_1,
        v1.datetime_chegada as chegada_1,
        v2.datetime_partida as partida_2,
        v2.datetime_chegada as chegada_2
    from base as v1
    inner join
        base as v2
        on v1.id_veiculo = v2.id_veiculo
        and v1.id_viagem < v2.id_viagem
        and v2.data in (
            v1.data,
            date_sub(v1.data, interval 1 day),
            date_add(v1.data, interval 1 day)
        )
        and datetime_diff(
            least(v1.datetime_chegada, v2.datetime_chegada),
            greatest(v1.datetime_partida, v2.datetime_partida),
            second
        )
        > 0
{% endtest %}
