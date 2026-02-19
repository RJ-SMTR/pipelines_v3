{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        tags=["geolocalizacao"],
        require_partition_filter=true,
    )
}}

{% set aux_gps_validador = ref("aux_gps_validador") %}


{% set incremental_filter %}
    ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    and datetime_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

-- busca quais partições serão atualizadas pelas capturas
{% if execute and is_incremental() %}
    {% set columns = (
        list_columns()
        | reject(
            "in",
            ["versao", "datetime_ultima_atualizacao", "id_execucao_dbt"],
        )
        | list
    ) %}
    {% set sha_column %}
        sha256(
            concat(
                {% for c in columns %}
                    ifnull(cast({{ c }} as string), 'n/a')
                    {% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        )
    {% endset %}
    {% set partitions_query %}
            select distinct
                concat("'", date(datetime_gps), "'") as data
            from {{ aux_gps_validador }}
            where {{ incremental_filter }}

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    dados_novos as (
        select
            modo,
            extract(date from datetime_gps) as data,
            extract(hour from datetime_gps) as hora,
            datetime_gps,
            datetime_captura,
            id_operadora,
            id_operadora_jae,
            operadora,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            case
                when modo = "VLT"
                then substring(id_veiculo, 1, 3)
                when modo = "BRT"
                then null
                else id_veiculo
            end as id_veiculo,
            id_validador,
            id_transmissao_gps,
            latitude,
            longitude,
            sentido,
            estado_equipamento,
            temperatura,
            versao_app
        from {{ aux_gps_validador }}
        where
            modo = "Van"
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    ),
    {% if is_incremental() and partitions | length > 0 %}
        dados_atuais as (
            select * from {{ this }} where data in ({{ partitions | join(", ") }})
        ),
    {% endif %}
    particao_completa as (
        select *, 0 as priority
        from dados_novos

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select
                * except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais
        {% endif %}
    ),
    gps_deduplicado as (
        select * except (priority), {{ sha_column }} as sha_dado_novo
        from particao_completa
        qualify
            row_number() over (
                partition by id_transmissao_gps order by datetime_captura desc, priority
            )
            = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() and partitions | length > 0 %}
            select
                id_transmissao_gps,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_transmissao_gps,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual

        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_transmissao_gps)
        from gps_deduplicado n
        left join sha_dados_atuais a using (id_transmissao_gps)
    ),
    gps_colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual
            ),
            '{{ var("version") }}' as versao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then current_datetime("America/Sao_Paulo")
                else datetime_ultima_atualizacao_atual
            end as datetime_ultima_atualizacao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then '{{ invocation_id }}'
                else id_execucao_dbt_atual
            end as id_execucao_dbt
        from sha_dados_completos
    )
select *
from gps_colunas_controle
