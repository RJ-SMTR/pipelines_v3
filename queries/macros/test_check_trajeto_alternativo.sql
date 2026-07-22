{% test test_check_trajeto_alternativo(model, column_name) %}
    with
        trips as (
            select
                trip_short_name as servico,
                regexp_replace(
                    normalize(
                        lower(regexp_extract({{ column_name }}, r'(\[.*?\])')), nfd
                    ),
                    r'\pM',
                    ''
                ) as evento,
                count(*) as quantidade_trips
            from {{ model }}
            where
                feed_start_date = '{{ var("data_versao_gtfs") }}'
                and regexp_extract({{ column_name }}, r'(\[.*?\])') is not null
            group by all
        ),
        ordem_servico_trajeto_alternativo_sentido as (
            select
                servico,
                regexp_replace(
                    normalize(lower(regexp_extract(evento, r'(\[.*?\])')), nfd),
                    r'\pM',
                    ''
                ) as evento,
                count(*) as quantidade_os
            from {{ ref("ordem_servico_trajeto_alternativo_sentido") }}
            where
                feed_start_date = '{{ var("data_versao_gtfs") }}'
                and regexp_extract(evento, r'(\[.*?\])') is not null
            group by all
        )
    select *
    from ordem_servico_trajeto_alternativo_sentido
    left join trips using (servico, evento)
    where quantidade_trips is null
{% endtest %}
