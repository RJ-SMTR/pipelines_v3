select
    date '2025-08-01' as data,
    'A29200_antes_v20' as id_veiculo,
    2019 as ano_fabricacao,
    struct(
        struct(
            date '2025-08-01' as data_verificacao_falha, true as valor
        ) as indicador_falha_recorrente
    ) as indicadores
union all
select
    date '2026-03-01',
    'A29200_nula_falha_v22',
    2023,
    struct(
        struct(
            date '2026-03-01' as data_verificacao_falha, true as valor
        ) as indicador_falha_recorrente
    )
