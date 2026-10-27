select
    date '2026-08-02' as data,
    'A29200_pre_v12_so_jae' as id_viagem,
    'A29200j' as id_veiculo,
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
    'A29200_pre_v12_sem' as id_viagem,
    'A29200k' as id_veiculo,
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
