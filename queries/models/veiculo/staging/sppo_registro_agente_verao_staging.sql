{{ config(materialized="view", alias="sppo_registro_agente_verao") }}

with

    veiculo as (
        select *
        from {{ source("veiculo_staging", "sppo_registro_agente_verao") }}

        union all
        select *
        from {{ source("source_veiculo", "sppo_registro_agente_verao") }}
    )

select distinct
    safe_cast(parse_datetime("%d/%m/%Y %H:%M:%S", datetime_registro) as date) as data,
    safe_cast(
        parse_datetime("%d/%m/%Y %H:%M:%S", datetime_registro) as datetime
    ) as datetime_registro,

    -- fmt: off
    sha256(
      parse_datetime("%d/%m/%Y %H:%M:%S", datetime_registro)
      || "_"
      || safe_cast(email AS string)
    ) AS id_registro,
    -- fmt: on
    safe_cast(json_value(content, '$.id_veiculo') as string) as id_veiculo,
    safe_cast(json_value(content, '$.servico') as string) as servico,
    safe_cast(json_value(content, '$.link_foto') as string) as link_foto,
    safe_cast(json_value(content, '$.validacao') as bool) as validacao,
    date(data) as data_captura,
    safe_cast(
        datetime(
            timestamp_trunc(timestamp(timestamp_captura), second), "America/Sao_Paulo"
        ) as datetime
    ) as datetime_captura,
    "{{ var('version') }}" as versao
from veiculo
