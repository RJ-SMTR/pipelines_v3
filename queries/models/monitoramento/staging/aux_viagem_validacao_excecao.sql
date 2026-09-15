{{ config(materialized="ephemeral") }}

/*
Para uma única data, use a mesma data em data_inicio e data_fim.
Fonte de GPS nula aplica a exceção a todos os fornecedores.
*/
with
    dados_manuais as (
        select *
        from
            unnest(
                cast(
                    [
                        -- struct(
                        -- date("2026-09-01") as data_inicio,
                        -- date("2026-09-03") as data_fim,
                        -- cast(null as string) as fonte_gps,
                        -- 10 as prazo_envio_dias,
                        -- cast(null as date) as data_limite_envio
                        -- )
                        struct(
                            date("2026-08-23") as data_inicio,
                            date("2026-08-31") as data_fim,
                            "maxtrack" as fonte_gps,
                            17 as prazo_envio_dias,
                            cast(null as date) as data_limite_envio
                        ),
                        -- Ofício SMTR nº 8656/2026 (Consórcios nº 143/2026)
                        -- Processo 000301.015075/2026-48
                        struct(
                            date("2026-08-15") as data_inicio,
                            date("2026-08-31") as data_fim,
                            cast(null as string) as fonte_gps,
                            cast(null as int64) as prazo_envio_dias,
                            date("2026-09-06") as data_limite_envio
                        ),
                        -- Ofício SMTR nº 9523/2026 (Consórcios nº 159/2026)
                        -- Processo 000301.015961/2026-71
                        struct(
                            date("2026-09-01") as data_inicio,
                            date("2026-09-15") as data_fim,
                            cast(null as string) as fonte_gps,
                            cast(null as int64) as prazo_envio_dias,
                            date("2026-09-21") as data_limite_envio
                        )
                    ] as array<
                        struct<
                            data_inicio date,
                            data_fim date,
                            fonte_gps string,
                            prazo_envio_dias int64,
                            data_limite_envio date
                        >
                    >
                )
            )
    )
select data_inicio, data_fim, fonte_gps, prazo_envio_dias, data_limite_envio
from dados_manuais
