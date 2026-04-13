{{
    config(
        alias="integracao_transacao",
    )
}}

select
    data,
    safe_cast(id as string) as id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            safe_cast(json_value(content, '$.data_inclusao') as string)
        ),
        'America/Sao_Paulo'
    ) as data_inclusao,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            safe_cast(json_value(content, '$.data_processamento') as string)
        ),
        'America/Sao_Paulo'
    ) as data_processamento,
    -- Seleciona colunas com os dados de cada transação da integração com os tipos
    -- adequados com base no dicionario de parametros
    {% for column, column_config in var("colunas_integracao").items() %}
        {% for i in range(var("quantidade_integracoes_max")) %}
            {% if column_config.type == "DATETIME" %}
                datetime(
                    parse_timestamp(
                        '%Y-%m-%dT%H:%M:%E*S%Ez',
                        safe_cast(
                            json_value(
                                content,
                                '$.{{ column }}_t{% if i > 0 %}i{% endif %}{{ i }}'
                            ) as string
                        )
                    ),
                    'America/Sao_Paulo'
                ) as {{ column }}_t{{ i }},
            {% elif column_config.type == "ID" %}
                replace(
                    safe_cast(
                        json_value(
                            content, '$.{{ column }}_t{% if i > 0 %}i{% endif %}{{ i }}'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as {{ column }}_t{{ i }},
            {% else %}
                safe_cast(
                    json_value(
                        content, '$.{{ column }}_t{% if i > 0 %}i{% endif %}{{ i }}'
                    ) as {{ column_config.type }}
                ) as {{ column }}_t{{ i }},
            {% endif %}
        {% endfor %}
    {% endfor %}
    safe_cast(
        json_value(content, '$.id_status_integracao') as string
    ) as id_status_integracao,
    safe_cast(
        json_value(content, '$.valor_transacao_total') as float64
    ) as valor_transacao_total,
    safe_cast(json_value(content, '$.tx_adicional') as string) as tx_adicional
from {{ source("source_jae", "integracao_transacao") }}
