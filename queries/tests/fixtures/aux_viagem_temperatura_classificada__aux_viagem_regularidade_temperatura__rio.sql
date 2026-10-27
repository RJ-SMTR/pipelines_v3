select
    date '2026-08-25' as data,
    'A2-050_rio_regular' as id_viagem,
    'A2-050' as id_veiculo,
    'V1' as id_validador,
    datetime '2026-08-25 01:59:04' as datetime_partida,
    datetime '2026-08-25 02:56:13' as datetime_chegada,
    'Ônibus' as modo,
    2026 as ano_fabricacao,
    'BASICO' as tecnologia_apurada,
    'MIDI' as tecnologia_remunerada,
    cast(null as string) as tipo_viagem,
    'SN898' as servico,
    'V' as sentido,
    32.658 as distancia_planejada,
    parse_json(
        '{"indicador_temperatura_nula_viagem":{"valor":false},"indicador_temperatura_regular_viagem":{"valor":true},"indicador_temperatura_transmitida_viagem":{"valor":true},"indicador_temperatura_zero_viagem":{"valor":false}}'
    ) as indicadores
union all
select
    date '2026-08-25',
    'A2-050_rio_zero',
    'A2-050z',
    'V1',
    datetime '2026-08-25 01:59:04',
    datetime '2026-08-25 02:56:13',
    'Ônibus',
    2026,
    'BASICO',
    'MIDI',
    cast(null as string),
    'SN898',
    'V',
    32.658,
    parse_json(
        '{"indicador_temperatura_nula_viagem":{"valor":false},"indicador_temperatura_regular_viagem":{"valor":true},"indicador_temperatura_transmitida_viagem":{"valor":true},"indicador_temperatura_zero_viagem":{"valor":true}}'
    )
