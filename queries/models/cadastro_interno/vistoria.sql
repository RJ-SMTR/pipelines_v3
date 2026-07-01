{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}
select
    data,
    id_vistoria,
    anoexe as ano_exercicio,
    safe_cast(json_value(content, '$.tptran1') as string) as id_tipo_transporte,
    safe_cast(json_value(content, '$.tptran2') as string) as id_tipo_transporte_2,
    safe_cast(json_value(content, '$.guia') as string) as guia,
    safe_cast(json_value(content, '$.selo') as string) as selo,
    safe_cast(json_value(content, '$.anoselo') as string) as ano_selo,
    safe_cast(json_value(content, '$.darm') as string) as darm,
    safe_cast(json_value(content, '$.anodarm') as string) as ano_darm,
    safe_cast(json_value(content, '$.placa') as string) as placa,
    safe_cast(json_value(content, '$.ordem') as string) as ordem,
    safe_cast(json_value(content, '$.cod_fiscal') as string) as cod_fiscal,
    safe_cast(json_value(content, '$.tptran') as string) as tptran,
    safe_cast(json_value(content, '$.tpperm') as string) as tpperm,
    safe_cast(json_value(content, '$.termo') as string) as termo,
    replace(safe_cast(json_value(content, '$.ratr') as string), '.0', '') as ratr,
    safe_cast(json_value(content, '$.visobrig') as string) as vistoria_obrigatoria,
    safe_cast(json_value(content, '$.num_vistoria') as string) as numero_vistoria,
    date(safe_cast(json_value(content, '$.dt_vistoria') as string)) as data_vistoria,
    safe_cast(json_value(content, '$.matr_usuario') as string) as matricula_usuario,
    date(safe_cast(json_value(content, '$.data_usuario') as string)) as data_usuario,
    safe_cast(json_value(content, '$.situac') as string) as situacao,
    safe_cast(json_value(content, '$.codrec') as string) as codrec,
    safe_cast(json_value(content, '$.Emissao') as string) as emissao,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    timestamp_captura
from {{ source("source_stu", "vistoria") }}
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
