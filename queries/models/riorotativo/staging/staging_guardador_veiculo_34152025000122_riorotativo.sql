{{ config(alias="guardador_veiculo_34152025000122") }}

select
    data,
    lpad(
        nullif(regexp_replace(safe_cast(cpf as string), r'[^0-9]', ''), ''), 11, '0'
    ) as cpf,
    lpad(
        nullif(
            regexp_replace(
                safe_cast(json_value(content, '$.identificacao') as string),
                r'[^0-9]',
                ''
            ),
            ''
        ),
        4,
        '0'
    ) as identificacao,
    "34152025000122" as cnpj,
    timestamp_captura
from {{ source("source_riorotativo", "entidade_34152025000122") }}
