select
    date '2025-10-15' as data,
    'A29200_antes_v19' as id_viagem,
    'A29200' as id_veiculo,
    'V1' as id_validador,
    datetime '2025-10-15 05:09:15' as datetime_partida,
    datetime '2025-10-15 05:29:39' as datetime_chegada,
    'Ônibus' as modo,
    2025 as ano_fabricacao,
    'BASICO' as tecnologia_apurada,
    'BASICO' as tecnologia_remunerada,
    'Licenciado com ar e não autuado' as tipo_viagem,
    'LECD136' as servico,
    'V' as sentido,
    13.716 as distancia_planejada,
    parse_json(
        '{"indicador_temperatura_nula_viagem":{"valor":false},"indicador_temperatura_regular_viagem":{"valor":true},"indicador_temperatura_transmitida_viagem":{"valor":true},"indicador_temperatura_zero_viagem":{"valor":false}}'
    ) as indicadores
union all
select
    date '2025-10-15',
    'A29200_ano_2019',
    'A29200_2019',
    'V1',
    datetime '2025-10-15 05:09:15',
    datetime '2025-10-15 05:29:39',
    'Ônibus',
    2019,
    'BASICO',
    'BASICO',
    'Licenciado com ar e não autuado',
    'LECD136',
    'V',
    13.716,
    parse_json(
        '{"indicador_temperatura_nula_viagem":{"valor":false},"indicador_temperatura_regular_viagem":{"valor":true},"indicador_temperatura_transmitida_viagem":{"valor":true},"indicador_temperatura_zero_viagem":{"valor":false}}'
    )
union all
select
    date '2025-08-01',
    'A29200_antes_v20_falha',
    'A29200_antes_v20',
    'V1',
    datetime '2025-08-01 05:09:15',
    datetime '2025-08-01 05:29:39',
    'Ônibus',
    2019,
    'BASICO',
    'BASICO',
    'Licenciado com ar e não autuado',
    'LECD136',
    'V',
    13.716,
    parse_json(
        '{"indicador_temperatura_nula_viagem":{"valor":false},"indicador_temperatura_regular_viagem":{"valor":true},"indicador_temperatura_transmitida_viagem":{"valor":true},"indicador_temperatura_zero_viagem":{"valor":false}}'
    )
union all
select
    date '2026-03-01',
    'A29200_nula_sem_falha_v22',
    'A29200_nula_v22',
    'V1',
    datetime '2026-03-01 05:09:15',
    datetime '2026-03-01 05:29:39',
    'Ônibus',
    2025,
    'BASICO',
    'BASICO',
    'Licenciado com ar e não autuado',
    'LECD136',
    'V',
    13.716,
    parse_json(
        '{"indicador_temperatura_nula_viagem":{"valor":true},"indicador_temperatura_regular_viagem":{"valor":true},"indicador_temperatura_transmitida_viagem":{"valor":true},"indicador_temperatura_zero_viagem":{"valor":false}}'
    )
union all
select
    date '2026-03-01',
    'A29200_nula_com_falha_v22',
    'A29200_nula_falha_v22',
    'V1',
    datetime '2026-03-01 05:09:15',
    datetime '2026-03-01 05:29:39',
    'Ônibus',
    2025,
    'BASICO',
    'BASICO',
    'Licenciado com ar e não autuado',
    'LECD136',
    'V',
    13.716,
    parse_json(
        '{"indicador_temperatura_nula_viagem":{"valor":true},"indicador_temperatura_regular_viagem":{"valor":true},"indicador_temperatura_transmitida_viagem":{"valor":true},"indicador_temperatura_zero_viagem":{"valor":false}}'
    )
union all
select
    date '2026-08-02',
    'A29200_flags_nulas',
    'A29200_nulos',
    'V1',
    datetime '2026-08-02 05:09:15',
    datetime '2026-08-02 05:29:39',
    'Ônibus',
    2025,
    'BASICO',
    'BASICO',
    'Licenciado com ar e não autuado',
    'LECD136',
    'V',
    13.716,
    parse_json('{}')
