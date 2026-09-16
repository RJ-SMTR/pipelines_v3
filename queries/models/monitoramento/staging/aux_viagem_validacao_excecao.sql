{{ config(materialized="ephemeral") }}

/*
Para uma única data, use a mesma data em data_inicio e data_fim.
Fonte de viagem nula aplica a exceção a todos os fornecedores.
*/
with
    dados_manuais as (
        select *
        from
            unnest(
                cast(
                    [
                        struct(
                            date("2026-08-16") as data_inicio,
                            date("2026-08-31") as data_fim,
                            "rioonibus" as fonte_viagem,
                            date("2026-09-06") as data_limite_envio -- Processo 000301.015075/2026-48
                        ),
                        struct(
                            date("2026-08-16") as data_inicio,
                            date("2026-08-31") as data_fim,
                            "maxtrack" as fonte_viagem,
                            date("2026-09-15") as data_limite_envio
                        ),
                        -- Ofício SMTR nº 9523/2026 (Consórcios nº 159/2026)
                        -- Processo 000301.015961/2026-71
                        struct(
                            date("2026-09-01") as data_inicio,
                            date("2026-09-15") as data_fim,
                            "rioonibus" as fonte_viagem,
                            date("2026-09-21") as data_limite_envio
                        )
                    ] as array<
                        struct<
                            data_inicio date,
                            data_fim date,
                            fonte_viagem string,
                            data_limite_envio date
                        >
                    >
                )
            )
    )
select data_inicio, data_fim, fonte_viagem, data_limite_envio
from dados_manuais
