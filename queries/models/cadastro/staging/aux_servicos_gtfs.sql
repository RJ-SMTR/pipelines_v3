{{
    config(
        materialized="ephemeral",
    )
}}

select
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude,
    inicio_vigencia,
    fim_vigencia,
    tabela_origem_gtfs,
    '{{ var("version") }}' as versao
from {{ ref("aux_routes_vigencia_gtfs") }}

union all

select
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude,
    inicio_vigencia,
    fim_vigencia,
    tabela_origem_gtfs,
    '{{ var("version") }}' as versao
from {{ ref("aux_stops_vigencia_gtfs") }}
