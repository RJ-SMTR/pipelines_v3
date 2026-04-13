{{
    config(
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}
{% set transacao_table = ref("transacao") %}
{% set integracao_table = ref("integracao") %}

{% if execute and is_incremental() %}

    {% set partitions_query %}
        SELECT
            CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
        FROM
            `{{ transacao_table.database }}.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE
            table_name = "{{ transacao_table.identifier }}"
            AND partition_id != "__NULL__"
            AND DATE(last_modified_time, "America/Sao_Paulo") = DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)

        UNION DISTINCT

        SELECT
            CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
        FROM
            `{{ integracao_table.database }}.{{ integracao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE
            table_name = "{{ integracao_table.identifier }}"
            AND partition_id != "__NULL__"
            AND DATE(last_modified_time, "America/Sao_Paulo") = DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}
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
{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}

{% endif %}


with
    integracao as (
        select id_transacao, id_integracao
        from {{ integracao_table }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} false
                {% endif %}
        {% endif %}
    ),
    transacao as (
        select
            t.data,
            t.hora,
            t.datetime_transacao,
            t.datetime_processamento,
            t.datetime_captura,
            t.modo,
            t.id_consorcio,
            t.consorcio,
            t.id_operadora,
            t.operadora,
            t.id_servico_jae,
            t.servico_jae,
            t.descricao_servico_jae,
            t.id_transacao,
            t.tipo_transacao,
            t.longitude,
            t.latitude,
            ifnull(t.longitude, 0) as longitude_tratada,
            ifnull(t.latitude, 0) as latitude_tratada,
            s.longitude as longitude_servico,
            s.latitude as latitude_servico,
            s.id_servico_gtfs,
            s.id_servico_jae as id_servico_jae_cadastro,
            s.tabela_origem_gtfs,
            i.id_integracao
        from {{ transacao_table }} t
        left join
            {{ ref("servicos") }} s
            on t.id_servico_jae = s.id_servico_jae
            and t.data >= s.data_inicio_vigencia
            and (t.data <= s.data_fim_vigencia or s.data_fim_vigencia is null)
        left join integracao i using (id_transacao)
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} false
                {% endif %}
        {% endif %}
    ),
    indicadores as (
        select
            * except (
                id_servico_gtfs,
                latitude_tratada,
                longitude_tratada,
                id_servico_jae_cadastro,
                tabela_origem_gtfs,
                tipo_transacao,
                id_integracao
            ),
            latitude_tratada = 0
            or longitude_tratada = 0 as indicador_geolocalizacao_zerada,
            (
                (latitude_tratada != 0 or longitude_tratada != 0)
                and not st_intersectsbox(
                    st_geogpoint(longitude_tratada, latitude_tratada),
                    -43.87,
                    -23.13,
                    -43.0,
                    -22.59
                )
            ) as indicador_geolocalizacao_fora_rio,
            (
                latitude_tratada != 0
                and longitude_tratada != 0
                and latitude_servico is not null
                and longitude_servico is not null
                and modo = "BRT"
                and tabela_origem_gtfs = "stops"
                and st_distance(
                    st_geogpoint(longitude_tratada, latitude_tratada),
                    st_geogpoint(longitude_servico, latitude_servico)
                )
                > 100
            ) as indicador_geolocalizacao_fora_stop,
            id_servico_gtfs is null
            and id_servico_jae_cadastro is not null
            and modo in ("Ônibus", "BRT") as indicador_servico_fora_gtfs,
            id_servico_jae_cadastro is null as indicador_servico_fora_vigencia,
            datetime_transacao
            > datetime_processamento as indicador_processamento_anterior_transacao,
            tipo_transacao = "Integração"
            and id_integracao is null as indicador_integracao_fora_tabela
        from transacao
    ),
    dados_novos as (
        select
            * except (
                indicador_servico_fora_gtfs,
                indicador_servico_fora_vigencia,
                indicador_processamento_anterior_transacao,
                indicador_integracao_fora_tabela
            ),
            case
                when indicador_geolocalizacao_zerada
                then "Geolocalização zerada"
                when indicador_geolocalizacao_fora_rio
                then "Geolocalização fora do município"
                when indicador_geolocalizacao_fora_stop
                then "Geolocalização fora do stop"
            end as descricao_geolocalizacao_invalida,
            indicador_servico_fora_gtfs,
            indicador_servico_fora_vigencia,
            indicador_processamento_anterior_transacao,
            indicador_integracao_fora_tabela
        from indicadores
        where
            indicador_geolocalizacao_zerada
            or indicador_geolocalizacao_fora_rio
            or indicador_geolocalizacao_fora_stop
            or indicador_servico_fora_gtfs
            or indicador_servico_fora_vigencia
            or indicador_processamento_anterior_transacao
            or indicador_integracao_fora_tabela
    ),
    {% if is_incremental() %}

        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} false
                {% endif %}

        ),
    {% endif %}
    particoes_completas as (
        select *, 0 as priority
        from dados_novos

        {% if is_incremental() %}
            union all

            select
                * except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais

        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from particoes_completas
        qualify row_number() over (partition by id_transacao order by priority) = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_transacao,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_transacao,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_transacao)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_transacao)
    ),
    colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual,
                priority
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
from colunas_controle
