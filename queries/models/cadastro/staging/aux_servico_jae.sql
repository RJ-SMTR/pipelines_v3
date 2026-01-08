{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_inicio_validade",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

with
    staging as (
        select
            date(timestamp_captura) as data_inicio_validade,
            cd_linha as id_servico_jae,
            nr_linha as servico_jae,
            nm_linha as descricao_servico_jae,
            gtfs_route_id,
            gtfs_stop_id,
            datetime_inclusao,
            timestamp_captura as datetime_inicio_validade,
            datetime(null) as datetime_fim_validade
        from {{ ref("staging_linha") }}
        {% if is_incremental() %}
            where
                data between date('{{ var("date_range_start") }}') and date(
                    '{{ var("date_range_end") }}'
                )
        {% endif %}
    ),
    dados_completos as (
        select *
        from staging
        {% if is_incremental() %}
            union all
            select *
            from {{ this }}
        {% endif %}
    ),
    dados_completos_sha as (
        select
            *,
            sha256(
                concat(
                    ifnull(id_servico_jae, 'n/a'),
                    ifnull(servico_jae, 'n/a'),
                    ifnull(descricao_servico_jae, 'n/a'),
                    ifnull(gtfs_route_id, 'n/a'),
                    ifnull(gtfs_stop_id, 'n/a'),
                    ifnull(cast(datetime_inclusao as string), 'n/a')
                )
            ) as sha_dado
        from dados_completos
    ),
    mudancas as (
        select * except (sha_dado)
        from dados_completos_sha
        qualify
            sha_dado != ifnull(
                lag(sha_dado) over (
                    partition by id_servico_jae order by datetime_inicio_validade
                ),
                cast('' as bytes)
            )
    )
select
    * except (datetime_fim_validade),
    lead(datetime_inicio_validade) over (
        partition by id_servico_jae order by datetime_inicio_validade
    ) as datetime_fim_validade
from mudancas
