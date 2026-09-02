{{ config(materialized="view", alias="viagem_onibus") }}

with
    viagem_completa as (
        select
            data,
            consorcio,
            "Ônibus" as modo,
            tipo_dia,
            id_empresa,
            id_veiculo,
            id_viagem,
            servico_informado,
            servico_realizado,
            servico_realizado as servico,
            vista,
            shape_id,
            sentido,
            datetime_partida,
            datetime_chegada,
            inicio_periodo,
            fim_periodo,
            tempo_viagem,
            distancia_planejada,
            perc_conformidade_shape,
            perc_conformidade_registros,
            versao_modelo
        from {{ ref("viagem_completa") }}
        where date(datetime_partida) < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    ),
    viagem_valida as (
        select
            data,
            consorcio,
            modo,
            tipo_dia,
            regexp_extract(upper(id_veiculo), r"^[A-Z]([0-9]{3})") as id_empresa,
            id_veiculo,
            id_viagem,
            servico as servico_informado,
            servico as servico_realizado,
            servico,
            vista,
            shape_id,
            sentido,
            datetime_partida,
            datetime_chegada,
            cast(null as datetime) as inicio_periodo,
            cast(null as datetime) as fim_periodo,
            datetime_diff(datetime_chegada, datetime_partida, minute) as tempo_viagem,
            distancia_planejada,
            cast(null as float64) as perc_conformidade_shape,
            cast(null as float64) as perc_conformidade_registros,
            versao as versao_modelo
        from {{ ref("viagem_valida") }}
        where date(datetime_partida) >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    )
select *
from viagem_completa
union all by name
select *
from viagem_valida
