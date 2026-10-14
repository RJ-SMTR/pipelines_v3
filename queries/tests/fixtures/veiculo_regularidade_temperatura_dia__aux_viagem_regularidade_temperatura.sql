select
    date '2026-08-02' as data,
    'A29042' as id_veiculo,
    2023 as ano_fabricacao,
    struct(
        struct(
            date '2026-08-04' as data_processamento_licenciamento, true as valor
        ) as indicador_ar_condicionado,
        struct(
            date '2026-08-25' as data_verificacao_regularidade, true as valor
        ) as indicador_temperatura_variacao_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_regularidade, true as valor
        ) as indicador_temperatura_transmitida_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_regularidade,
            true as valor,
            72.46 as percentual_temperatura_pos_tratamento_descartada
        ) as indicador_temperatura_descartada_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_regularidade,
            true as valor,
            72.22 as percentual_viagem_temperatura_pos_tratamento_descartada
        ) as indicador_viagem_temperatura_descartada_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_falha, false as valor
        ) as indicador_falha_recorrente
    ) as indicadores
union all
select
    date '2026-08-02',
    'A29200_falha_recorrente',
    2023,
    struct(
        struct(
            date '2026-08-04' as data_processamento_licenciamento, true as valor
        ) as indicador_ar_condicionado,
        struct(
            date '2026-08-25' as data_verificacao_regularidade, true as valor
        ) as indicador_temperatura_variacao_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_regularidade, true as valor
        ) as indicador_temperatura_transmitida_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_regularidade,
            true as valor,
            72.46 as percentual_temperatura_pos_tratamento_descartada
        ) as indicador_temperatura_descartada_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_regularidade,
            true as valor,
            72.22 as percentual_viagem_temperatura_pos_tratamento_descartada
        ) as indicador_viagem_temperatura_descartada_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_falha, true as valor
        ) as indicador_falha_recorrente
    )
union all
select
    date '2026-08-02',
    'A29200_nula_falha_recorrente',
    2023,
    struct(
        struct(
            date '2026-08-04' as data_processamento_licenciamento, true as valor
        ) as indicador_ar_condicionado,
        struct(
            date '2026-08-25' as data_verificacao_regularidade, true as valor
        ) as indicador_temperatura_variacao_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_regularidade, true as valor
        ) as indicador_temperatura_transmitida_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_regularidade,
            true as valor,
            72.46 as percentual_temperatura_pos_tratamento_descartada
        ) as indicador_temperatura_descartada_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_regularidade,
            true as valor,
            72.22 as percentual_viagem_temperatura_pos_tratamento_descartada
        ) as indicador_viagem_temperatura_descartada_veiculo,
        struct(
            date '2026-08-25' as data_verificacao_falha, true as valor
        ) as indicador_falha_recorrente
    )
