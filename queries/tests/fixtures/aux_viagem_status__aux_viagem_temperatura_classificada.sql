select
    date '2026-08-25' as data,
    'SN898' as servico,
    datetime '2026-08-25 01:59:04' as datetime_partida,
    datetime '2026-08-25 02:10:04' as datetime_chegada,
    'A2-050' as id_veiculo,
    'UQO2G60' as placa,
    2026 as ano_fabricacao,
    'A2-050_rio_classificada' as id_viagem,
    parse_json('{"indicador_ar_condicionado":{"valor":true}}') as indicadores,
    32.658 as distancia_planejada,
    'BASICO' as tecnologia_apurada,
    'V' as sentido,
    'Ônibus' as modo
