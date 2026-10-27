select
    date '2026-08-25' as data,
    'NONE' as id_veiculo,
    2026 as ano_fabricacao,
    struct(
        struct(
            date '2026-08-25' as data_verificacao_falha, false as valor
        ) as indicador_falha_recorrente
    ) as indicadores
limit 0
