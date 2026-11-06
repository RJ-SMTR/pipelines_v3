select
    date '2026-08-02' as data,
    'A29200_LECD136_V_5dva_20260802050915' as id_viagem,
    'A29200' as id_veiculo,
    'TUI8B82' as placa,
    2025 as ano_fabricacao,
    datetime '2026-08-02 05:09:15' as datetime_partida,
    datetime '2026-08-02 05:29:39' as datetime_chegada,
    'Ônibus' as modo,
    'BASICO' as tecnologia_apurada,
    'BASICO' as tecnologia_remunerada,
    'Licenciado com ar e não autuado' as tipo_viagem,
    parse_json(
        '{"indicador_ar_condicionado":{"data_processamento_licenciamento":"2026-08-04","valor":true},"indicador_autuacao_ar_condicionado":{"data_inclusao_datalake_autuacao":null,"valor":false},"indicador_licenciado":{"data_processamento_licenciamento":"2026-08-04","valor":true},"indicador_penalidade_tecnologia":{"data_inicio_vigencia":"2026-07-19","valor":false},"indicador_registro_agente_verao_ar_condicionado":{"data_registro_agente_verao":null,"valor":false},"indicador_veiculo_lacrado":{"data_processamento_licenciamento":"2026-08-04","valor":false},"indicador_vistoriado":{"data_processamento_licenciamento":"2026-08-04","valor":true}}'
    ) as indicadores,
    'LECD136' as servico,
    'V' as sentido,
    13.716 as distancia_planejada
union all
select
    date '2026-08-02',
    'A29042_161_C_hz72_20260802105622',
    'A29042',
    'SRA6D54',
    2023,
    datetime '2026-08-02 10:56:22',
    datetime '2026-08-02 12:06:51',
    'Ônibus',
    'BASICO',
    'BASICO',
    'Licenciado com ar e não autuado',
    parse_json(
        '{"indicador_ar_condicionado":{"data_processamento_licenciamento":"2026-08-04","valor":true},"indicador_autuacao_ar_condicionado":{"data_inclusao_datalake_autuacao":null,"valor":false},"indicador_licenciado":{"data_processamento_licenciamento":"2026-08-04","valor":true},"indicador_penalidade_tecnologia":{"data_inicio_vigencia":"2026-07-19","valor":false},"indicador_registro_agente_verao_ar_condicionado":{"data_registro_agente_verao":null,"valor":false},"indicador_veiculo_lacrado":{"data_processamento_licenciamento":"2026-08-04","valor":false},"indicador_vistoriado":{"data_processamento_licenciamento":"2026-08-04","valor":true}}'
    ),
    '161',
    'C',
    26.583
