{{ config(alias="guardador_veiculo_05019730000158") }}

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
    "05019730000158" as cnpj,
    timestamp_captura
from {{ source("source_riorotativo", "entidade_05019730000158") }}
