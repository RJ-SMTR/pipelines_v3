select
    date '2026-08-02' as data,
    'A29200_LECD136_V_5dva_20260802050915' as id_viagem,
    'A29200' as id_veiculo,
    'B35T020D00208174' as id_validador,
    datetime '2026-08-02 05:09:15' as datetime_partida,
    datetime '2026-08-02 05:29:39' as datetime_chegada,
    'Ônibus' as modo,
    2025 as ano_fabricacao,
    'BASICO' as tecnologia_apurada,
    'BASICO' as tecnologia_remunerada,
    'Licenciado com ar e não autuado' as tipo_viagem,
    'LECD136' as servico,
    'V' as sentido,
    13.716 as distancia_planejada,
    parse_json(
        '{"indicador_ar_condicionado":{"data_processamento_licenciamento":"2026-08-04","valor":true},"indicador_autuacao_ar_condicionado":{"data_inclusao_datalake_autuacao":null,"valor":false},"indicador_licenciado":{"data_processamento_licenciamento":"2026-08-04","valor":true},"indicador_penalidade_tecnologia":{"data_inicio_vigencia":"2026-07-19","valor":false},"indicador_registro_agente_verao_ar_condicionado":{"data_registro_agente_verao":null,"valor":false},"indicador_temperatura_nula_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","percentual_temperatura_nula_descartada":"0","valor":false},"indicador_temperatura_pos_tratamento_descartada_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","percentual_temperatura_pos_tratamento_descartada":"0","valor":false},"indicador_temperatura_regular_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","percentual_temperatura_regular":"100","valor":true},"indicador_temperatura_transmitida_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","valor":true},"indicador_temperatura_variacao_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","valor":true},"indicador_temperatura_zero_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","percentual_temperatura_zero_descartada":"0","valor":false},"indicador_validador":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","valores":[{"id_validador":"B35T020D00208174","indicador_estado_equipamento_aberto":true,"indicador_gps_servico_divergente":false,"indicador_temperatura_nula_viagem":false,"indicador_temperatura_pos_tratamento_descartada_viagem":false,"indicador_temperatura_regular_viagem":true,"indicador_temperatura_transmitida_viagem":true,"indicador_temperatura_variacao_viagem":true,"indicador_temperatura_zero_viagem":false,"percentual_estado_equipamento_aberto":"100","percentual_temperatura_nula_descartada":"0","percentual_temperatura_pos_tratamento_descartada":"0","percentual_temperatura_regular":100,"percentual_temperatura_zero_descartada":"0","quantidade_nula":0,"quantidade_pos_tratamento":20,"quantidade_pre_tratamento":20,"quantidade_zero":0}]},"indicador_veiculo_lacrado":{"data_processamento_licenciamento":"2026-08-04","valor":false},"indicador_vistoriado":{"data_processamento_licenciamento":"2026-08-04","valor":true}}'
    ) as indicadores
union all
select
    date '2026-08-02',
    'A29042_161_C_hz72_20260802105622',
    'A29042',
    'B35T020D00208278',
    datetime '2026-08-02 10:56:22',
    datetime '2026-08-02 12:06:51',
    'Ônibus',
    2023,
    'BASICO',
    'BASICO',
    'Licenciado com ar e não autuado',
    '161',
    'C',
    26.583,
    parse_json(
        '{"indicador_ar_condicionado":{"data_processamento_licenciamento":"2026-08-04","valor":true},"indicador_autuacao_ar_condicionado":{"data_inclusao_datalake_autuacao":null,"valor":false},"indicador_licenciado":{"data_processamento_licenciamento":"2026-08-04","valor":true},"indicador_penalidade_tecnologia":{"data_inicio_vigencia":"2026-07-19","valor":false},"indicador_registro_agente_verao_ar_condicionado":{"data_registro_agente_verao":null,"valor":false},"indicador_temperatura_nula_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","percentual_temperatura_nula_descartada":"0","valor":false},"indicador_temperatura_pos_tratamento_descartada_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","percentual_temperatura_pos_tratamento_descartada":"91.17","valor":true},"indicador_temperatura_regular_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","percentual_temperatura_regular":"16.66","valor":false},"indicador_temperatura_transmitida_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","valor":true},"indicador_temperatura_variacao_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","valor":true},"indicador_temperatura_zero_viagem":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","percentual_temperatura_zero_descartada":"0","valor":false},"indicador_validador":{"datetime_verificacao_regularidade":"2026-08-25 19:17:30.428136","valores":[{"id_validador":"B35T020D00208278","indicador_estado_equipamento_aberto":true,"indicador_gps_servico_divergente":false,"indicador_temperatura_nula_viagem":false,"indicador_temperatura_pos_tratamento_descartada_viagem":true,"indicador_temperatura_regular_viagem":false,"indicador_temperatura_transmitida_viagem":true,"indicador_temperatura_variacao_viagem":true,"indicador_temperatura_zero_viagem":false,"percentual_estado_equipamento_aberto":"100","percentual_temperatura_nula_descartada":"0","percentual_temperatura_pos_tratamento_descartada":"91.17","percentual_temperatura_regular":16.66,"percentual_temperatura_zero_descartada":"0","quantidade_nula":0,"quantidade_pos_tratamento":6,"quantidade_pre_tratamento":68,"quantidade_zero":0}]},"indicador_veiculo_lacrado":{"data_processamento_licenciamento":"2026-08-04","valor":false},"indicador_vistoriado":{"data_processamento_licenciamento":"2026-08-04","valor":true}}'
    )
union all
select
    date '2026-08-02',
    'A29200_LECD136_V_5dva_20260802050915_temperatura_zero',
    'A29200',
    'B35T020D00208174',
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
    parse_json(
        '{"indicador_ar_condicionado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_autuacao_ar_condicionado": {"data_inclusao_datalake_autuacao": null, "valor": false}, "indicador_licenciado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_penalidade_tecnologia": {"data_inicio_vigencia": "2026-07-19", "valor": false}, "indicador_registro_agente_verao_ar_condicionado": {"data_registro_agente_verao": null, "valor": false}, "indicador_temperatura_nula_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_nula_descartada": "0", "valor": false}, "indicador_temperatura_pos_tratamento_descartada_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_pos_tratamento_descartada": "0", "valor": false}, "indicador_temperatura_regular_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_regular": "100", "valor": true}, "indicador_temperatura_transmitida_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": true}, "indicador_temperatura_variacao_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": true}, "indicador_temperatura_zero_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_zero_descartada": "0", "valor": true}, "indicador_validador": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valores": [{"id_validador": "B35T020D00208174", "indicador_estado_equipamento_aberto": true, "indicador_gps_servico_divergente": false, "indicador_temperatura_nula_viagem": false, "indicador_temperatura_pos_tratamento_descartada_viagem": false, "indicador_temperatura_regular_viagem": true, "indicador_temperatura_transmitida_viagem": true, "indicador_temperatura_variacao_viagem": true, "indicador_temperatura_zero_viagem": false, "percentual_estado_equipamento_aberto": "100", "percentual_temperatura_nula_descartada": "0", "percentual_temperatura_pos_tratamento_descartada": "0", "percentual_temperatura_regular": 100, "percentual_temperatura_zero_descartada": "0", "quantidade_nula": 0, "quantidade_pos_tratamento": 20, "quantidade_pre_tratamento": 20, "quantidade_zero": 0}]}, "indicador_veiculo_lacrado": {"data_processamento_licenciamento": "2026-08-04", "valor": false}, "indicador_vistoriado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}}'
    )
union all
select
    date '2026-08-02',
    'A29200_LECD136_V_5dva_20260802050915_temperatura_nao_transmitida',
    'A29200',
    'B35T020D00208174',
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
    parse_json(
        '{"indicador_ar_condicionado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_autuacao_ar_condicionado": {"data_inclusao_datalake_autuacao": null, "valor": false}, "indicador_licenciado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_penalidade_tecnologia": {"data_inicio_vigencia": "2026-07-19", "valor": false}, "indicador_registro_agente_verao_ar_condicionado": {"data_registro_agente_verao": null, "valor": false}, "indicador_temperatura_nula_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_nula_descartada": "0", "valor": false}, "indicador_temperatura_pos_tratamento_descartada_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_pos_tratamento_descartada": "0", "valor": false}, "indicador_temperatura_regular_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_regular": "100", "valor": true}, "indicador_temperatura_transmitida_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": false}, "indicador_temperatura_variacao_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": true}, "indicador_temperatura_zero_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_zero_descartada": "0", "valor": false}, "indicador_validador": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valores": [{"id_validador": "B35T020D00208174", "indicador_estado_equipamento_aberto": true, "indicador_gps_servico_divergente": false, "indicador_temperatura_nula_viagem": false, "indicador_temperatura_pos_tratamento_descartada_viagem": false, "indicador_temperatura_regular_viagem": true, "indicador_temperatura_transmitida_viagem": true, "indicador_temperatura_variacao_viagem": true, "indicador_temperatura_zero_viagem": false, "percentual_estado_equipamento_aberto": "100", "percentual_temperatura_nula_descartada": "0", "percentual_temperatura_pos_tratamento_descartada": "0", "percentual_temperatura_regular": 100, "percentual_temperatura_zero_descartada": "0", "quantidade_nula": 0, "quantidade_pos_tratamento": 20, "quantidade_pre_tratamento": 20, "quantidade_zero": 0}]}, "indicador_veiculo_lacrado": {"data_processamento_licenciamento": "2026-08-04", "valor": false}, "indicador_vistoriado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}}'
    )
union all
select
    date '2026-08-02',
    'A29200_LECD136_V_5dva_20260802050915_temperatura_nula',
    'A29200',
    'B35T020D00208174',
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
    parse_json(
        '{"indicador_ar_condicionado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_autuacao_ar_condicionado": {"data_inclusao_datalake_autuacao": null, "valor": false}, "indicador_licenciado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_penalidade_tecnologia": {"data_inicio_vigencia": "2026-07-19", "valor": false}, "indicador_registro_agente_verao_ar_condicionado": {"data_registro_agente_verao": null, "valor": false}, "indicador_temperatura_nula_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_nula_descartada": "0", "valor": true}, "indicador_temperatura_pos_tratamento_descartada_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_pos_tratamento_descartada": "0", "valor": false}, "indicador_temperatura_regular_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_regular": "100", "valor": true}, "indicador_temperatura_transmitida_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": true}, "indicador_temperatura_variacao_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": true}, "indicador_temperatura_zero_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_zero_descartada": "0", "valor": false}, "indicador_validador": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valores": [{"id_validador": "B35T020D00208174", "indicador_estado_equipamento_aberto": true, "indicador_gps_servico_divergente": false, "indicador_temperatura_nula_viagem": false, "indicador_temperatura_pos_tratamento_descartada_viagem": false, "indicador_temperatura_regular_viagem": true, "indicador_temperatura_transmitida_viagem": true, "indicador_temperatura_variacao_viagem": true, "indicador_temperatura_zero_viagem": false, "percentual_estado_equipamento_aberto": "100", "percentual_temperatura_nula_descartada": "0", "percentual_temperatura_pos_tratamento_descartada": "0", "percentual_temperatura_regular": 100, "percentual_temperatura_zero_descartada": "0", "quantidade_nula": 0, "quantidade_pos_tratamento": 20, "quantidade_pre_tratamento": 20, "quantidade_zero": 0}]}, "indicador_veiculo_lacrado": {"data_processamento_licenciamento": "2026-08-04", "valor": false}, "indicador_vistoriado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}}'
    )
union all
select
    date '2026-08-02',
    'A29200_LECD136_V_5dva_20260802050915_falha_recorrente',
    'A29200_falha_recorrente',
    'B35T020D00208174',
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
    parse_json(
        '{"indicador_ar_condicionado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_autuacao_ar_condicionado": {"data_inclusao_datalake_autuacao": null, "valor": false}, "indicador_licenciado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_penalidade_tecnologia": {"data_inicio_vigencia": "2026-07-19", "valor": false}, "indicador_registro_agente_verao_ar_condicionado": {"data_registro_agente_verao": null, "valor": false}, "indicador_temperatura_nula_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_nula_descartada": "0", "valor": false}, "indicador_temperatura_pos_tratamento_descartada_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_pos_tratamento_descartada": "0", "valor": false}, "indicador_temperatura_regular_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_regular": "100", "valor": true}, "indicador_temperatura_transmitida_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": true}, "indicador_temperatura_variacao_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": true}, "indicador_temperatura_zero_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_zero_descartada": "0", "valor": false}, "indicador_validador": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valores": [{"id_validador": "B35T020D00208174", "indicador_estado_equipamento_aberto": true, "indicador_gps_servico_divergente": false, "indicador_temperatura_nula_viagem": false, "indicador_temperatura_pos_tratamento_descartada_viagem": false, "indicador_temperatura_regular_viagem": true, "indicador_temperatura_transmitida_viagem": true, "indicador_temperatura_variacao_viagem": true, "indicador_temperatura_zero_viagem": false, "percentual_estado_equipamento_aberto": "100", "percentual_temperatura_nula_descartada": "0", "percentual_temperatura_pos_tratamento_descartada": "0", "percentual_temperatura_regular": 100, "percentual_temperatura_zero_descartada": "0", "quantidade_nula": 0, "quantidade_pos_tratamento": 20, "quantidade_pre_tratamento": 20, "quantidade_zero": 0}]}, "indicador_veiculo_lacrado": {"data_processamento_licenciamento": "2026-08-04", "valor": false}, "indicador_vistoriado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}}'
    )
union all
select
    date '2026-08-02',
    'A29200_LECD136_V_5dva_20260802050915_temperatura_nula_falha_recorrente',
    'A29200_nula_falha_recorrente',
    'B35T020D00208174',
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
    parse_json(
        '{"indicador_ar_condicionado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_autuacao_ar_condicionado": {"data_inclusao_datalake_autuacao": null, "valor": false}, "indicador_licenciado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}, "indicador_penalidade_tecnologia": {"data_inicio_vigencia": "2026-07-19", "valor": false}, "indicador_registro_agente_verao_ar_condicionado": {"data_registro_agente_verao": null, "valor": false}, "indicador_temperatura_nula_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_nula_descartada": "0", "valor": true}, "indicador_temperatura_pos_tratamento_descartada_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_pos_tratamento_descartada": "0", "valor": false}, "indicador_temperatura_regular_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_regular": "100", "valor": true}, "indicador_temperatura_transmitida_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": true}, "indicador_temperatura_variacao_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valor": true}, "indicador_temperatura_zero_viagem": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "percentual_temperatura_zero_descartada": "0", "valor": false}, "indicador_validador": {"datetime_verificacao_regularidade": "2026-08-25 19:17:30.428136", "valores": [{"id_validador": "B35T020D00208174", "indicador_estado_equipamento_aberto": true, "indicador_gps_servico_divergente": false, "indicador_temperatura_nula_viagem": false, "indicador_temperatura_pos_tratamento_descartada_viagem": false, "indicador_temperatura_regular_viagem": true, "indicador_temperatura_transmitida_viagem": true, "indicador_temperatura_variacao_viagem": true, "indicador_temperatura_zero_viagem": false, "percentual_estado_equipamento_aberto": "100", "percentual_temperatura_nula_descartada": "0", "percentual_temperatura_pos_tratamento_descartada": "0", "percentual_temperatura_regular": 100, "percentual_temperatura_zero_descartada": "0", "quantidade_nula": 0, "quantidade_pos_tratamento": 20, "quantidade_pre_tratamento": 20, "quantidade_zero": 0}]}, "indicador_veiculo_lacrado": {"data_processamento_licenciamento": "2026-08-04", "valor": false}, "indicador_vistoriado": {"data_processamento_licenciamento": "2026-08-04", "valor": true}}'
    )
