{{
    config(
        alias="operadora_pessoa_fisica",
    )
}}

with
    operadora_pessoa_fisica as (
        select
            data,
            safe_cast(perm_autor as string) as perm_autor,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) as timestamp_captura,
            safe_cast(json_value(content, '$.CPF') as string) as cpf,
            parse_date(
                '%d/%m/%Y', left(safe_cast(json_value(content, '$.Data') as string), 10)
            ) as data_registro,
            safe_cast(json_value(content, '$.Ratr') as string) as ratr,
            safe_cast(json_value(content, '$.Processo') as string) as processo,
            safe_cast(json_value(content, '$.Nome') as string) as nome,
            safe_cast(json_value(content, '$.Placa') as string) as placa,
            safe_cast(json_value(content, '$.id_modo') as string) as id_modo,
            safe_cast(json_value(content, '$.modo') as string) as modo,
            safe_cast(
                json_value(content, '$.tipo_permissao') as string
            ) as tipo_permissao
        from {{ source("br_rj_riodejaneiro_stu_staging", "operadora_pessoa_fisica") }}
    ),
    operadora_pessoa_fisica_rn as (
        select
            *,
            row_number() over (
                partition by coalesce(cpf, perm_autor), modo
                order by timestamp_captura desc, data_registro desc
            ) as rn
        from operadora_pessoa_fisica
    )
select * except (rn)
from operadora_pessoa_fisica_rn
where rn = 1
