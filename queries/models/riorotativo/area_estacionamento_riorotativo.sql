{{
    config(
        materialized="table",
        alias="area_estacionamento",
        enabled=is_current_state_enabled(),
    )
}}

/* backfill com janela antiga desliga o modelo: ver macro is_current_state_enabled */
{% if execute %}
    {% set last_partition_query %}
        select max(data)
        from {{ ref("staging_area_estacionamento_riorotativo") }}
        where data between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}")
    {% endset %}
    {% set last_partition = run_query(last_partition_query).columns[0].values()[0] %}
    {% if last_partition is none %}
        {{
            exceptions.raise_compiler_error(
                "No staging_area_estacionamento_riorotativo partitions found between date_range_start and date_range_end"
            )
        }}
    {% endif %}
{% endif %}

select
    area_codigo as id_area,
    area_nome as nome,
    area_logradouro as logradouro,
    area_endereco_referencia as endereco_referencia,
    area_poligono as geometry_wkt,
    st_geogfromtext(area_poligono, make_valid => true) as geometry,
    area_observacao as observacao,
    area_vaga_total as quantidade_vaga_total,
    area_vaga_moto as quantidade_vaga_moto,
    area_vaga_idoso as quantidade_vaga_idoso,
    area_vaga_pcd as quantidade_vaga_pcd,
    area_tempo_permanencia_hora as tempo_permanencia_hora,
    area_perfil_funcionamento as id_perfil_funcionamento,
    data_inicio_vigencia,
    data_fim_vigencia
from {{ ref("staging_area_estacionamento_riorotativo") }}
where data = date("{{ last_partition }}")
