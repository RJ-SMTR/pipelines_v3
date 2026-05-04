{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        tags=["geolocalizacao"],
    )
}}

-- depends_on: {{ ref('feed_info_gtfs') }}
{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

with
    aux_segmento as (
        select
            feed_start_date,
            feed_end_date,
            feed_version,
            shape_id,
            id_segmento,
            st_geogfromtext(wkt_segmento) as segmento,
            wkt_segmento,
            round(cast(comprimento_segmento as float64), 2) as comprimento_segmento,
            st_geogfromtext(buffer_completo) as buffer_completo
        from {{ ref("aux_segmento_shape") }}
    ),
    tunel as (
        select
            inicio_vigencia as inicio_vigencia_tunel,
            fim_vigencia as fim_vigencia_tunel,
            st_union_agg(
                st_buffer(geometry, {{ var("buffer_tunel_metros") }})
            ) as buffer_tunel
        from {{ ref("tuneis") }}
        where
            (date('{{ last_feed_version }}') between inicio_vigencia and fim_vigencia)
            or (
                date('{{ last_feed_version }}') >= inicio_vigencia
                and fim_vigencia is null
            )
            or (date('{{ last_feed_version }}') <= inicio_vigencia)
        group by 1, 2
    ),
    segmento_primeiro_ultimo as (
        select
            feed_start_date,
            shape_id,
            min(cast(id_segmento as integer)) as primeiro_segmento,
            max(cast(id_segmento as integer)) as ultimo_segmento
        from aux_segmento
        group by 1, 2
    ),
    intercessao_segmento as (
        select
            s1.feed_start_date,
            s1.shape_id,
            s1.id_segmento,
            st_union(array_agg(s2.buffer_completo)) as buffer_segmento_prioritario
        from aux_segmento s1
        join
            segmento_primeiro_ultimo l
            on s1.shape_id = l.shape_id
            and s1.feed_start_date = l.feed_start_date
        join
            aux_segmento s2
            on s1.feed_start_date = s2.feed_start_date
            and s1.shape_id = s2.shape_id
            and s1.id_segmento != s2.id_segmento
            and st_intersects(s1.buffer_completo, s2.buffer_completo)
            and (
                -- segmentos posteriores (logica atual)
                s1.id_segmento < s2.id_segmento
                -- primeiro segmento tem prioridade sobre os do meio
                or cast(s2.id_segmento as integer) = l.primeiro_segmento
            )
        where
            -- apenas segmentos do meio sao recortados
            cast(s1.id_segmento as integer) != l.primeiro_segmento
            and cast(s1.id_segmento as integer) != l.ultimo_segmento
        group by 1, 2, 3
    ),
    buffer_segmento_recortado as (
        select
            s.*,
            coalesce(
                st_difference(s.buffer_completo, i.buffer_segmento_prioritario),
                s.buffer_completo
            ) as buffer
        from aux_segmento s
        left join intercessao_segmento i using (feed_start_date, shape_id, id_segmento)
    ),
    indicador_validacao_shape as (
        select
            s.*,
            t.inicio_vigencia_tunel,
            t.fim_vigencia_tunel,
            coalesce(
                st_intersects(s.segmento, t.buffer_tunel), false
            ) as indicador_tunel,
            st_area(s.buffer) / st_area(s.buffer_completo)
            < {{ var("limite_reducao_area_buffer") }} as indicador_area_prejudicada,
            s.comprimento_segmento
            < {{ var("comprimento_minimo_segmento_shape") }}
            as indicador_segmento_pequeno,
            cast(id_segmento as integer) as id_segmento_int
        from buffer_segmento_recortado s
        left join tunel t on st_intersects(s.segmento, t.buffer_tunel)
    )
select
    * except (id_segmento_int),
    (
        (
            indicador_tunel
            and (
                (id_segmento_int > 1)
                and (
                    id_segmento_int
                    < max(id_segmento_int) over (partition by feed_start_date, shape_id)
                )
            )
        )
        or indicador_area_prejudicada
        or indicador_segmento_pequeno
    ) as indicador_segmento_desconsiderado,
    '{{ var("version") }}' as versao
from indicador_validacao_shape

{% if is_incremental() %}

    union all

    select
        s.feed_start_date,
        fi.feed_end_date,
        s.feed_version,
        s.shape_id,
        s.id_segmento,
        s.segmento,
        s.wkt_segmento,
        s.comprimento_segmento,
        s.buffer_completo,
        s.buffer,
        s.inicio_vigencia_tunel,
        s.fim_vigencia_tunel,
        s.indicador_tunel,
        s.indicador_area_prejudicada,
        s.indicador_segmento_pequeno,
        s.indicador_segmento_desconsiderado,
        s.versao
    from {{ this }} s
    join {{ ref("feed_info_gtfs") }} fi using (feed_start_date)
    {# join `rj-smtr.gtfs.feed_info` fi using (feed_start_date) #}
    where feed_start_date = '{{ last_feed_version }}'

{% endif %}
