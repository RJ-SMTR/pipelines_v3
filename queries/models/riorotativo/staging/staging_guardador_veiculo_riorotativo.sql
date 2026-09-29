{{ config(alias="guardador_veiculo") }}

with
    guardadores as (
        select
            data,
            lpad(
                regexp_replace(safe_cast(cpf as string), r'[^0-9]', ''), 11, '0'
            ) as cpf,
            lpad(
                regexp_replace(
                    safe_cast(json_value(content, '$.identificacao') as string),
                    r'[^0-9]',
                    ''
                ),
                4,
                '0'
            ) as identificacao,
            "05019730000158" as cnpj,
            timestamp_captura
        from {{ source("source_riorotativo", "entidade_05019730000158") }}

        union all

        select
            data,
            lpad(
                regexp_replace(safe_cast(cpf as string), r'[^0-9]', ''), 11, '0'
            ) as cpf,
            lpad(
                regexp_replace(
                    safe_cast(json_value(content, '$.identificacao') as string),
                    r'[^0-9]',
                    ''
                ),
                4,
                '0'
            ) as identificacao,
            "34152025000122" as cnpj,
            timestamp_captura
        from {{ source("source_riorotativo", "entidade_34152025000122") }}
    ),
    dados as (
        select *
        from guardadores
        qualify
            row_number() over (
                partition by data, cnpj, cpf order by timestamp_captura desc
            )
            = 1
    )
select *
from dados
