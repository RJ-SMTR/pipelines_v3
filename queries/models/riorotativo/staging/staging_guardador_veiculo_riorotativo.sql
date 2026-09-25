{{ config(alias="guardador_veiculo") }}

with
    dados as (
        select
            data,
            lpad(
                regexp_replace(safe_cast(identificacao as string), r'[^0-9]', ''),
                4,
                '0'
            ) as numero_identificacao,
            lpad(
                regexp_replace(safe_cast(cpf as string), r'[^0-9]', ''), 11, '0'
            ) as documento,
            "CPF" as tipo_documento,
            cnpj,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) as datetime_captura
        from {{ ref("base_guardador_veiculo_riorotativo") }}
    )
select *
from dados
qualify
    row_number() over (
        partition by data, cnpj, documento order by datetime_captura desc
    )
    = 1
