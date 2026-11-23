select
    date '2026-08-02' as data,
    'A29200_base' as id_viagem,
    'A29200b' as id_veiculo,
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
    'A29200_riocard' as id_viagem,
    'A29200r' as id_veiculo,
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
    'A29200_fora_janela' as id_viagem,
    'A29200f' as id_veiculo,
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
    'A29200_atraso' as id_viagem,
    'A29200a' as id_veiculo,
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
    'A29200_modo' as id_viagem,
    'A29200m' as id_veiculo,
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
    'A29200_divergente' as id_viagem,
    'A29200d' as id_veiculo,
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
    'A29200_fechado' as id_viagem,
    'A29200c' as id_veiculo,
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
    'A29200_gps_div' as id_viagem,
    'A29200g' as id_veiculo,
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
    'A29200_multiplos' as id_viagem,
    'A29200p' as id_veiculo,
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
    'A29200_sem_validador' as id_viagem,
    'A29200v' as id_veiculo,
    datetime '2026-08-02 05:09:15' as datetime_partida,
    datetime '2026-08-02 05:29:39' as datetime_chegada,
    'Ônibus' as modo,
    'BASICO' as tecnologia_apurada,
    'BASICO' as tecnologia_remunerada,
    'Licenciado com ar e não autuado' as tipo_viagem,
    'LECD136' as servico,
    'V' as sentido,
    13.716 as distancia_planejada,
    parse_json('{"indicador_validador":{"valores":[]}}') as indicadores
union all
select
    date '2026-08-02' as data,
    'A29200_sem_transacao' as id_viagem,
    'A29200s' as id_veiculo,
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
