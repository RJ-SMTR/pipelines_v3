{{ config(materialized="ephemeral") }}

/*
Cada exceção deve ser adicionada ao array abaixo como um intervalo inclusivo e
com o prazo excepcional em dias corridos.
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
                    -- 10 as prazo_envio_dias
                    -- )
                    ] as array<
                        struct<
                            data_inicio date,
                            data_fim date,
                            fonte_gps string,
                            prazo_envio_dias int64
                        >
                    >
                )
            )
    )
select data_inicio, data_fim, fonte_gps, prazo_envio_dias
from dados_manuais
