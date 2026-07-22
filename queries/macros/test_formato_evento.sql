{% test test_formato_evento(model, column_name) %}
    select
        feed_start_date,
        tipo_os,
        servico,
        sentido,
        {{ column_name }} as evento,
        regexp_replace(
            regexp_replace(
                normalize(lower(safe_cast({{ column_name }} as string)), nfd),
                r'\pM',
                ''
            ),
            r'[^a-z0-9\[\]]+',
            '_'
        ) as evento_corrigido
    from {{ model }}
    where
        feed_start_date = '{{ var("data_versao_gtfs") }}'
        and not regexp_contains(
            safe_cast({{ column_name }} as string), r'^\[[a-z0-9_]+\]$'
        )
{% endtest %}
