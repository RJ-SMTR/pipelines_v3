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
        viagem_informada,
        truncate_date=true,
        max_age_days=var("viagem_validacao_max_age_days", 5),
    ) %}
    {% set expanded_partitions = get_modified_partitions_filter(
        viagem_informada,
        include_adjacent=true,
        truncate_date=true,
        max_age_days=var("viagem_validacao_max_age_days", 5),
    ) %}
{% else %} {% set partitions = [] %} {% set expanded_partitions = [] %}
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

{% set gps_filter %}
    {% if is_incremental() %}
        {% if expanded_partitions | length > 0 %} data in ({{ expanded_partitions | join(", ") }})
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
            fonte_gps,
            fonte_viagem
        from {{ ref("viagem_informada_monitoramento") }}
        {# from `rj-smtr.monitoramento.viagem_informada` #}
        where {{ incremental_filter }}
    ),
    gps_onibus as (
        select
            data,
            datetime_gps,
            servico,
            id_veiculo,
            latitude,
            longitude,
            fonte_gps as fornecedor
        from {{ ref("view_gps_onibus") }}
        where {{ gps_filter }}
    ),
    gps_brt as (
        select
            data,
            timestamp_gps as datetime_gps,
            servico,
            id_veiculo,
            latitude,
            longitude,
            'brt' as fornecedor
        from {{ ref("view_gps_brt_completo") }}
        where {{ gps_filter }}
    ),
    gps_jae as (
        select
            data,
            datetime_gps,
            servico_jae as servico,
            case
                when id_operadora = '2801'
                then 'A2-' || lpad(right(id_veiculo, 3), 3, '0')
                when id_operadora = '2802'
                then 'B2-' || lpad(right(id_veiculo, 3), 3, '0')
                else null
            end as id_veiculo,
            latitude,
            longitude,
            'jae' as fornecedor
        from {{ ref("gps_validador") }}
        where
            {{ gps_filter }}
            and id_operadora in ('2801', '2802')
            and latitude != 0
            and longitude != 0
            and id_veiculo != '99999'
            and data <= "2026-09-16"
    /*
        2801 - GTU (A2)
        2802 - TUSE (B2)
    */
    ),
    gps_union as (
        select *
        from gps_onibus

        union all

        select *
        from gps_brt

        union all

        select *
        from gps_jae
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
    g.fornecedor as fonte_gps,
    v.fonte_viagem,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from gps_union g
join
    viagem v
    on g.datetime_gps between v.datetime_partida and v.datetime_chegada
    and g.id_veiculo = v.id_veiculo
    and (
        g.fornecedor = v.fonte_gps
        or (g.fornecedor = 'jae' and v.fonte_gps = 'maxtrack')
    )
{% if not is_incremental() %}
    where v.data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
{% endif %}
