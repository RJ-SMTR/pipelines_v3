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


{% set incremental_filter %}
    {% if is_incremental() %}
        data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
    {% else %} data >= date('{{ var("data_inicial_gps_validacao_viagem") }}')
    {% endif %}
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
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
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
        {# from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_brt` #}
        from {{ ref("gps_brt") }}
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
    where v.data <= date_sub(current_date("America/Sao_Paulo"), interval 2 day)
{% endif %}
