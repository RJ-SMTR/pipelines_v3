{{
    config(
        alias="conta_bancaria",
    )
}}

with
    conta_bancaria as (
        select
            data,
            replace(safe_cast(cd_cliente as string), '.0', '') as cd_cliente,
            timestamp_captura,
            safe_cast(json_value(content, '$.CD_AGENCIA') as string) as cd_agencia,
            safe_cast(
                json_value(content, '$.CD_TIPO_CONTA') as string
            ) as cd_tipo_conta,
            safe_cast(json_value(content, '$.NM_BANCO') as string) as nm_banco,
            safe_cast(json_value(content, '$.NR_BANCO') as string) as nr_banco,
            safe_cast(json_value(content, '$.NR_CONTA') as string) as nr_conta,
        from {{ source("br_rj_riodejaneiro_bilhetagem_staging", "conta_bancaria") }}
    ),
    conta_bancaria_rn as (
        select
            *,
            row_number() over (
                partition by cd_cliente order by timestamp_captura desc
            ) as rn
        from conta_bancaria
    )
select * except (rn)
from conta_bancaria_rn
where rn = 1
