{{ config(materialized="ephemeral") }}

with
    guardadores as (
        select
            data,
            safe_cast(cpf as string) as cpf,
            safe_cast(
                json_value(content, '$.identificacao') as string
            ) as identificacao,
            "05019730000158" as cnpj,
            timestamp_captura
        from {{ source("source_riorotativo", "entidade_05019730000158") }}

        union all

        select
            data,
            safe_cast(cpf as string) as cpf,
            safe_cast(
                json_value(content, '$.identificacao') as string
            ) as identificacao,
            "34152025000122" as cnpj,
            timestamp_captura
        from {{ source("source_riorotativo", "entidade_34152025000122") }}
    )

select *
from guardadores
