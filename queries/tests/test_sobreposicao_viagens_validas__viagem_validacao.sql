{{
    config(
        description="Nenhum par de viagens válidas do mesmo veículo tem sobreposição superior a 300 segundos",
        severity="warn",
    )
}}

with
    /*
    Viagens válidas do período testado e dos dias vizinhos, para identificar
    sobreposições entre partições
    */
    viagens as (
        select
            data,
            id_viagem,
            id_veiculo,
            datetime_partida_considerada,
            datetime_chegada_considerada
        from {{ ref("viagem_validacao") }}
        where
            data between date_sub(
                date('{{ var("date_range_start") }}'), interval 1 day
            ) and date_add(date('{{ var("date_range_end") }}'), interval 1 day)
            and indicador_viagem_valida
    )
select
    v1.id_veiculo,
    v1.data,
    v1.id_viagem,
    v1.datetime_partida_considerada,
    v1.datetime_chegada_considerada,
    v2.data as data_sobreposta,
    v2.id_viagem as id_viagem_sobreposta,
    v2.datetime_partida_considerada as datetime_partida_considerada_sobreposta,
    v2.datetime_chegada_considerada as datetime_chegada_considerada_sobreposta,
    datetime_diff(
        least(v1.datetime_chegada_considerada, v2.datetime_chegada_considerada),
        greatest(v1.datetime_partida_considerada, v2.datetime_partida_considerada),
        second
    ) as segundos_sobreposicao
from viagens as v1
join
    viagens as v2
    on v1.id_veiculo = v2.id_veiculo
    and v1.id_viagem < v2.id_viagem
    and v1.datetime_partida_considerada < v2.datetime_chegada_considerada
    and v1.datetime_chegada_considerada > v2.datetime_partida_considerada
where
    (
        v1.data between date('{{ var("date_range_start") }}') and date(
            '{{ var("date_range_end") }}'
        )
        or v2.data between date('{{ var("date_range_start") }}') and date(
            '{{ var("date_range_end") }}'
        )
    )
    and datetime_diff(
        least(v1.datetime_chegada_considerada, v2.datetime_chegada_considerada),
        greatest(v1.datetime_partida_considerada, v2.datetime_partida_considerada),
        second
    )
    > 300
