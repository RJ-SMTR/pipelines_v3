select
    date '2026-08-02' as data,
    'A29200_v99_fechado' as id_viagem,
    'A29200w' as id_veiculo,
    datetime '2026-08-02 05:09:15' as datetime_partida,
    datetime '2026-08-02 05:29:39' as datetime_chegada,
    'Ônibus' as modo,
    'BASICO' as tecnologia_apurada,
    'BASICO' as tecnologia_remunerada,
    'Licenciado com ar e não autuado' as tipo_viagem,
    'LECD136' as servico,
    'V' as sentido,
    13.716 as distancia_planejada,
    parse_json(
        '{"indicador_validador":{"valores":[{"id_validador":"V1","percentual_estado_equipamento_aberto":"10","indicador_estado_equipamento_aberto":false,"indicador_gps_servico_divergente":false}]}}'
    ) as indicadores
union all
select
    date '2026-08-02' as data,
    'A29200_v99_gps_div' as id_viagem,
    'A29200x' as id_veiculo,
    datetime '2026-08-02 05:09:15' as datetime_partida,
    datetime '2026-08-02 05:29:39' as datetime_chegada,
    'Ônibus' as modo,
    'BASICO' as tecnologia_apurada,
    'BASICO' as tecnologia_remunerada,
    'Licenciado com ar e não autuado' as tipo_viagem,
    'LECD136' as servico,
    'V' as sentido,
    13.716 as distancia_planejada,
    parse_json(
        '{"indicador_validador":{"valores":[{"id_validador":"V1","percentual_estado_equipamento_aberto":"100","indicador_estado_equipamento_aberto":true,"indicador_gps_servico_divergente":true}]}}'
    ) as indicadores
union all
select
    date '2026-08-02' as data,
    'A29200_v99_servico_div' as id_viagem,
    'A29200y' as id_veiculo,
    datetime '2026-08-02 05:09:15' as datetime_partida,
    datetime '2026-08-02 05:29:39' as datetime_chegada,
    'Ônibus' as modo,
    'BASICO' as tecnologia_apurada,
    'BASICO' as tecnologia_remunerada,
    'Licenciado com ar e não autuado' as tipo_viagem,
    'LECD136' as servico,
    'V' as sentido,
    13.716 as distancia_planejada,
    parse_json(
        '{"indicador_validador":{"valores":[{"id_validador":"V1","percentual_estado_equipamento_aberto":"100","indicador_estado_equipamento_aberto":true,"indicador_gps_servico_divergente":false}]}}'
    ) as indicadores
union all
select
    date '2026-08-02' as data,
    'A29200_v99_multiplos' as id_viagem,
    'A29200z' as id_veiculo,
    datetime '2026-08-02 05:09:15' as datetime_partida,
    datetime '2026-08-02 05:29:39' as datetime_chegada,
    'Ônibus' as modo,
    'BASICO' as tecnologia_apurada,
    'BASICO' as tecnologia_remunerada,
    'Licenciado com ar e não autuado' as tipo_viagem,
    'LECD136' as servico,
    'V' as sentido,
    13.716 as distancia_planejada,
    parse_json(
        '{"indicador_validador":{"valores":[{"id_validador":"VA","percentual_estado_equipamento_aberto":"100","indicador_estado_equipamento_aberto":true,"indicador_gps_servico_divergente":false},{"id_validador":"VB","percentual_estado_equipamento_aberto":"0","indicador_estado_equipamento_aberto":false,"indicador_gps_servico_divergente":false}]}}'
    ) as indicadores
union all
select
    date '2026-08-02' as data,
    'A29200_v99_aberto' as id_viagem,
    'A29200u' as id_veiculo,
    datetime '2026-08-02 05:09:15' as datetime_partida,
    datetime '2026-08-02 05:29:39' as datetime_chegada,
    'Ônibus' as modo,
    'BASICO' as tecnologia_apurada,
    'BASICO' as tecnologia_remunerada,
    'Licenciado com ar e não autuado' as tipo_viagem,
    'LECD136' as servico,
    'V' as sentido,
    13.716 as distancia_planejada,
    parse_json(
        '{"indicador_validador":{"valores":[{"id_validador":"V1","percentual_estado_equipamento_aberto":"100","indicador_estado_equipamento_aberto":true,"indicador_gps_servico_divergente":false}]}}'
    ) as indicadores
