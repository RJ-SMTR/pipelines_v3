{{
    config(
        alias="operadora_transporte",
    )
}}

with
    operadora_transporte as (
        select
            data,
            replace(
                safe_cast(cd_operadora_transporte as string), ".0", ""
            ) as cd_operadora_transporte,
            timestamp_captura,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%S%Ez',
                    safe_cast(json_value(content, '$.DT_INCLUSAO') as string)
                ),
                "America/Sao_Paulo"
            ) as datetime_inclusao,
            replace(
                safe_cast(json_value(content, '$.CD_CLIENTE') as string), ".0", ""
            ) as cd_cliente,
            replace(
                safe_cast(json_value(content, '$.CD_TIPO_CLIENTE') as string), ".0", ""
            ) as cd_tipo_cliente,
            replace(
                safe_cast(json_value(content, '$.CD_TIPO_MODAL') as string), ".0", ""
            ) as cd_tipo_modal,
            safe_cast(
                json_value(content, '$.IN_SITUACAO_ATIVIDADE') as string
            ) as in_situacao_atividade,
            safe_cast(json_value(content, '$.DS_TIPO_MODAL') as string) as ds_tipo_modal
        from {{ source("source_jae", "operadora_transporte") }}
    ),
    operadora_transporte_rn as (
        select
            *,
            row_number() over (
                partition by cd_operadora_transporte order by timestamp_captura desc
            ) as rn
        from operadora_transporte
    )
select * except (rn)
from operadora_transporte_rn
where rn = 1
