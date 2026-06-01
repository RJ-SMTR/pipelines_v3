{{
    config(
        materialized="view",
    )
}}


with
    dados_manuais as (
        select *
        from
            unnest(
                [
                    struct(  -- Processo SEI_000301.005390_2026_67
                        '2026-03-18' as data_inicio,
                        '2026-03-18' as data_fim,
                        cast(null as string) as faixa_horaria_inicio,
                        cast(null as string) as faixa_horaria_fim,
                        [
                            "201",
                            "202",
                            "006",
                            "133",
                            "507",
                            "426",
                            "607",
                            "711",
                            "416"
                        ] as servicos,
                        0 as valor_penalidade
                    ),
                    struct(  -- Processo SEI_000301.005390_2026_67
                        '2026-03-19' as data_inicio,
                        '2026-03-19' as data_fim,
                        cast(null as string) as faixa_horaria_inicio,
                        cast(null as string) as faixa_horaria_fim,
                        ["133", "607", "711"] as servicos,
                        0 as valor_penalidade
                    ),
                    struct(  -- Processo SEI_000301.005390_2026_67
                        '2026-03-20' as data_inicio,
                        '2026-03-20' as data_fim,
                        cast(null as string) as faixa_horaria_inicio,
                        cast(null as string) as faixa_horaria_fim,
                        ["133", "607"] as servicos,
                        0 as valor_penalidade
                    )
                ]
            )
    )
select
    date(data_inicio) as data_inicio,
    date(data_fim) as data_fim,
    datetime(faixa_horaria_inicio) as faixa_horaria_inicio,
    datetime(faixa_horaria_fim) as faixa_horaria_fim,
    servico,
    valor_penalidade
from dados_manuais
left join unnest(servicos) as servico
