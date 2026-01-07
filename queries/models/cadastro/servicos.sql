{{ config(materialized="table", tags=["geolocalizacao"]) }}

with
    linha_jae as (
        select *
        from {{ ref("aux_servico_jae") }}
        qualify
            row_number() over (
                partition by id_servico_jae order by datetime_inicio_validade desc
            )
            = 1
    )
select
    g.id_servico as id_servico_gtfs,
    j.id_servico_jae,
    coalesce(g.servico, j.servico_jae) as servico,
    g.servico as servico_gtfs,
    j.servico_jae,
    coalesce(g.descricao_servico, j.descricao_servico_jae) as descricao_servico,
    g.descricao_servico as descricao_servico_gtfs,
    j.descricao_servico_jae,
    g.latitude,
    g.longitude,
    g.tabela_origem_gtfs,
    coalesce(g.inicio_vigencia, date(j.datetime_inclusao)) as data_inicio_vigencia,
    g.fim_vigencia as data_fim_vigencia,
    '{{ var("version") }}' as versao
from linha_jae j
full outer join
    {{ ref("aux_servicos_gtfs") }} g
    on coalesce(j.gtfs_route_id, j.gtfs_stop_id) = g.id_servico
