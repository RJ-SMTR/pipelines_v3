{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        require_partition_filter=true,
    )
}}

{% set viagem_informada = ref("viagem_informada_monitoramento") %}
{% if execute and is_incremental() %}
    {% set partitions = get_modified_partitions_filter(
        viagem_informada, truncate_date=true
    ) %}
{% else %} {% set partitions = [] %}
{% endif %}

{% set incremental_filter %}
    {% if is_incremental() %}
        {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
        {% else %} data = date("2000-01-01")
        {% endif %}
        and
    {% endif %}
    data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
{% endset %}

with
    viagem as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            modo,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            fonte_gps
        from {{ ref("viagem_informada_monitoramento") }}
        {# from `rj-smtr.monitoramento.viagem_informada` #}
        where {{ incremental_filter }}
    ),
    gps_conecta as (
        select data, datetime_gps, servico, id_veiculo, latitude, longitude
        {# from `rj-smtr.monitoramento.gps_onibus_conecta` #}
        from {{ source("monitoramento", "gps_onibus_conecta") }}
        where {{ incremental_filter }}

    ),
    gps_zirix as (
        select data, datetime_gps, servico, id_veiculo, latitude, longitude
        {# from `rj-smtr.monitoramento.gps_onibus_zirix` #}
        from {{ source("monitoramento", "gps_onibus_zirix") }}
        where {{ incremental_filter }}
    ),
    gps_cittati as (
        select data, datetime_gps, servico, id_veiculo, latitude, longitude
        {# from `rj-smtr.monitoramento.gps_onibus_cittati` #}
        from {{ source("monitoramento", "gps_onibus_cittati") }}
        where {{ incremental_filter }}
    ),
    gps_brt as (
        select
            data,
            timestamp_gps as datetime_gps,
            servico,
            id_veiculo,
            latitude,
            longitude
        from {{ ref("view_gps_brt_completo") }}
        where {{ incremental_filter }}
    ),
    gps_union as (
        select *, 'conecta' as fornecedor
        from gps_conecta

        union all

        select *, 'zirix' as fornecedor
        from gps_zirix

        union all

        select *, 'cittati' as fornecedor
        from gps_cittati

        union all

        select *, 'brt' as fornecedor
        from gps_brt
    )
select
    v.data,
    g.datetime_gps,
    v.modo,
    g.id_veiculo,
    v.servico as servico_viagem,
    g.servico as servico_gps,
    v.sentido,
    g.latitude,
    g.longitude,
    st_geogpoint(g.longitude, g.latitude) as geo_point_gps,
    v.id_viagem,
    v.datetime_partida,
    v.datetime_chegada,
    v.trip_id,
    v.route_id,
    v.shape_id,
    v.fonte_gps,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from gps_union g
join
    viagem v
    on g.datetime_gps between v.datetime_partida and v.datetime_chegada
    and g.id_veiculo = v.id_veiculo
    and g.fornecedor = v.fonte_gps
{% if not is_incremental() %}
    where v.data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
{% endif %}
