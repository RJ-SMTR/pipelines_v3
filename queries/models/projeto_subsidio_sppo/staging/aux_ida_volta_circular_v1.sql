{{
    config(
        materialized="ephemeral",
    )
}}

select
    *,
    lead(datetime_partida) over (
        partition by id_veiculo, servico_realizado
        order by id_veiculo, servico_realizado, datetime_partida, sentido_shape
    ) as datetime_partida_volta,
    lead(datetime_chegada) over (
        partition by id_veiculo, servico_realizado
        order by id_veiculo, servico_realizado, datetime_partida, sentido_shape
    ) as datetime_chegada_volta,
    lead(shape_id) over (
        partition by id_veiculo, servico_realizado
        order by id_veiculo, servico_realizado, datetime_partida, sentido_shape
    ) as shape_id_volta,
    lead(sentido_shape) over (
        partition by id_veiculo, servico_realizado
        order by id_veiculo, servico_realizado, datetime_partida, sentido_shape
    )
    = "V" as flag_proximo_volta
from {{ ref("aux_viagem_inicio_fim") }} v
where
    sentido = "C"
    and data < date("{{ var('DATA_SUBSIDIO_V24_INICIO') }}") -- Exceção devido ao recurso SMTR202507001429 
    and data not between date("2025-06-01")
    and date("2025-06-30")
