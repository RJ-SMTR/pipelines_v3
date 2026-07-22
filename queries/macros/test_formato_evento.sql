{% test test_formato_evento(model, column_name, date_column="feed_start_date") %}
    select
        safe_cast({{ date_column }} as date) as feed_start_date,
        safe_cast(tipo_os as string) as tipo_os,
        safe_cast(servico as string) as servico,
        safe_cast(sentido as string) as sentido,
        safe_cast({{ column_name }} as string) as evento,
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
        safe_cast({{ date_column }} as string) = '{{ var("data_versao_gtfs") }}'
        and not regexp_contains(
            safe_cast({{ column_name }} as string), r'^\[[a-z0-9_]+\]$'
        )
{% endtest %}
