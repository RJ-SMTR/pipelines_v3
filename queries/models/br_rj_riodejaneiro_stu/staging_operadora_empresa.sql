{{
    config(
        alias="operadora_empresa",
    )
}}
{# trigger testing #}
with
    operadora_empresa as (
        select
            data,
            safe_cast(perm_autor as string) as perm_autor,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) as timestamp_captura,
            safe_cast(json_value(content, '$.CNPJ') as string) as cnpj,
            date(
                parse_timestamp(
                    '%d/%m/%Y', safe_cast(json_value(content, '$.Data') as string)
                ),
                "America/Sao_Paulo"
            ) as data_registro,
            safe_cast(json_value(content, '$.Processo') as string) as processo,
            safe_cast(json_value(content, '$.Razao_Social') as string) as razao_social,
            safe_cast(json_value(content, '$.id_modo') as string) as id_modo,
            safe_cast(json_value(content, '$.modo') as string) as modo,
            safe_cast(
                json_value(content, '$.tipo_permissao') as string
            ) as tipo_permissao
        from {{ source("br_rj_riodejaneiro_stu_staging", "operadora_empresa") }}
    ),
    operadora_empresa_rn as (
        select
            *,
            row_number() over (
                partition by coalesce(cnpj, perm_autor), modo
                order by timestamp_captura desc, data_registro desc
            ) as rn
        from operadora_empresa
    )
select * except (rn)
from operadora_empresa_rn
where rn = 1
