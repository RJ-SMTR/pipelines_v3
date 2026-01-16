{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
        },
    )
}}

with
    source_previnity as (
        select data, safe_cast(data_autuacao as date) as data_autuacao, response, payload, "previnity" as fonte
        from {{ source("source_previnity", "retorno_negativacao") }}
        {% if is_incremental() %}
            where data = date('{{ var("date_range_end") }}')
        {% endif %}
    ),
    parsed as (
        select
            data,
            data_autuacao,
            json_extract_array(replace(response, "'", '"')) as response_array,
            replace(payload, "'", '"') as payload_json,
            fonte
        from source_previnity
    )
select
    data,
    data_autuacao,
    parse_datetime('%d/%m/%Y %H:%M:%S', json_value(r, '$.data')) as datetime_retorno,
    json_value(r, '$.produtonome') as produtonome,
    json_value(r, '$.produtoreferencia') as produtoreferencia,
    json_value(r, '$.protocolo') as protocolo,
    json_value(payload_json, '$.contrato') as id_auto_infracao,
    json_value(payload_json, '$.cpf') as cpf,
    json_value(r, '$.resultado') as resultado,
    fonte,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from parsed, unnest(response_array) as r
