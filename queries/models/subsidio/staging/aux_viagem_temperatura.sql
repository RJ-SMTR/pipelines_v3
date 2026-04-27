{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date_add(date("{{ var('date_range_end') }}"), interval 1 day) and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset -%}

{% set partition_filter %}
    data between date("{{var('date_range_start')}}") and date("{{ var('date_range_end') }}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

{% if execute %}
    {% if is_incremental() %}
        {% set columns = (
            list_columns()
            | reject(
                "in",
                [
                    "indicadores",
                    "versao",
                    "datetime_ultima_atualizacao",
                    "id_execucao_dbt",
                ],
            )
            | list
        ) + ["indicadores_str"] %}
        {% set sha_column %}
            sha256(
                concat(
                    {% for c in columns %}
                        {% if c == 'indicadores_str' %}
                            ifnull(
                                regexp_replace(
                                    cast({{ c }} as string),
                                    r'"datetime_verificacao_regularidade":"[^"]*",',
                                    ''
                                ),
                                'n/a'
                            )
                        {% else %}
                            ifnull(cast({{ c }} as string), 'n/a')
                        {% endif %}

                        {% if not loop.last %}, {% endif %}
                    {% endfor %}
                )
            )
        {% endset %}

        {% set partitions_query %}
            select distinct concat("'", data, "'") as data
            from {{ ref("viagem_classificada") }}
            where {{ partition_filter }}
        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% else %} {% set sha_column = "cast(null as bytes)" %}
    {% endif %}
{% endif %}

with
    viagens as (  -- Viagens realizadas no período de apuração
        select
            data,
            servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            placa,
            ano_fabricacao,
            id_viagem,
            tipo_viagem,
            indicadores,
            safe_cast(
                json_value(indicadores, '$.indicador_ar_condicionado.valor') as bool
            ) as indicador_ar_condicionado,
            distancia_planejada,
            tecnologia_apurada,
            tecnologia_remunerada,
            sentido,
            modo
        from {{ ref("viagem_classificada") }}
        {# from `rj-smtr.subsidio.viagem_classificada` #}
        where {{ partition_filter }}
    ),
    endereco_manutencao_validador as (  -- Geometria correspondente ao endereço de manutenção dos validadores conforme Ofício nº 165/2025/CBD
        select
            st_buffer(
                st_geogfromtext(
                    "POLYGON((-43.1809263629012 -22.9023415400994, -43.1809355158356 -22.9023188980249, -43.1809499203212 -22.9022832575615, -43.1809751620823 -22.9022207928068, -43.181136469863 -22.9022726961659, -43.1811457018491 -22.9022806583809, -43.1811533860381 -22.902289957837, -43.1811593104274 -22.9023003289061, -43.1811632944557 -22.9023114728602, -43.1811652249352 -22.9023230901438, -43.1811650686283 -22.9023348365165, -43.1810535167347 -22.9025089202062, -43.1810234734783 -22.9024966649362, -43.1808862935479 -22.9024406953277, -43.1808894789739 -22.9024328125621, -43.1809057097345 -22.902392650667, -43.1809263629012 -22.9023415400994))"
                ),
                10
            ) as geometry
    {# select st_buffer(geometry, 10) as geometry
        from datario.dados_mestres.lote
        where id_lote = "287B01604" #}
    ),
    garagens as (  -- Geometrias das garagens válidas no período de apuração
        select inicio_vigencia, fim_vigencia, st_union_agg(geometry) as geometry
        from {{ ref("garagem") }}
        {# from `rj-smtr.cadastro.garagem` #}
        where
            inicio_vigencia <= date('{{ var("date_range_end") }}')
            and (
                fim_vigencia is null
                or fim_vigencia >= date('{{ var("date_range_start") }}')
            )
        group by all
    ),
    agg_garagens_manutencao as (  -- Agrega geometrias de garagens e endereço de manutenção dos validadores
        select
            g.inicio_vigencia,
            g.fim_vigencia,
            st_union(g.geometry, e.geometry) as geometry
        from garagens g
        cross join endereco_manutencao_validador e
    ),
    gps_validador as (  -- Dados base de GPS, temperatura, etc
        select distinct
            data,
            datetime_gps,
            servico_jae,
            id_veiculo,
            id_validador,
            estado_equipamento,
            latitude,
            longitude,
            safe_cast(temperatura as numeric) as temperatura,
            datetime_captura
        from {{ ref("gps_validador") }}
        where {{ incremental_filter }} and modo = "Ônibus"
    ),
    estado_equipamento_aux as (  -- Dados de estado do equipamento do validador
        select *
        from
            (
                (
                    select
                        data,
                        servico_jae,
                        id_validador,
                        id_veiculo,
                        latitude,
                        longitude,
                        if(
                            count(case when estado_equipamento = "ABERTO" then 1 end)
                            >= 1,
                            "ABERTO",
                            "FECHADO"
                        ) as estado_equipamento,
                        min(datetime_gps) as datetime_gps
                    from gps_validador
                    where latitude != 0 and longitude != 0
                    group by all
                )
                union all
                (
                    select
                        data,
                        servico_jae,
                        id_validador,
                        id_veiculo,
                        latitude,
                        longitude,
                        estado_equipamento,
                        datetime_gps
                    from gps_validador
                    where latitude = 0 and longitude = 0
                )
            )
    ),
    gps_validador_bilhetagem_viagem as (  -- Dados de bilhetagem com base apenas nas viagens realizadas
        select
            v.data,
            e.datetime_gps,
            v.id_viagem,
            e.id_validador,
            e.estado_equipamento,
            e.latitude,
            e.longitude,
            st_geogpoint(e.longitude, e.latitude) as posicao_geo,
            v.servico,
            e.servico_jae
        from viagens as v
        left join
            estado_equipamento_aux as e
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    gps_validador_bilhetagem_viagem_filtrada as (  -- Filtra pontos de GPS fora das garagens e endereços de manutenção dos validadores
        select v.*
        from gps_validador_bilhetagem_viagem v
        where
            v.data < date("{{ var('DATA_SUBSIDIO_V21_INICIO') }}")
            or (
                v.data >= date("{{ var('DATA_SUBSIDIO_V21_INICIO') }}")
                and v.data < date("{{ var('DATA_SUBSIDIO_V19_INICIO') }}")
                and not exists (
                    select 1
                    from agg_garagens_manutencao g
                    where
                        (
                            v.data between g.inicio_vigencia and coalesce(
                                g.fim_vigencia, v.data
                            )
                        )
                        and st_intersects(v.posicao_geo, g.geometry)
                )
            )
            or v.data >= date("{{ var('DATA_SUBSIDIO_V19_INICIO') }}")
    ),
    indicador_equipamento_bilhetagem as (  -- Indicadores de estado do equipamento do validador por viagem
        select
            data,
            id_viagem,
            id_validador,
            countif(servico != servico_jae) > 0 as indicador_gps_servico_divergente,
            trunc(countif(estado_equipamento = "ABERTO") / count(*), 5)
            * 100 as percentual_estado_equipamento_aberto,
            (
                countif(estado_equipamento = "ABERTO") / count(*) >= 0.8
                or id_validador is null
            ) as indicador_estado_equipamento_aberto
        from gps_validador_bilhetagem_viagem_filtrada
        group by 1, 2, 3
    ),
    gps_validador_viagem as (  -- Dados completos de GPS, temperatura e bilhetagem por viagem realizada
        select
            v.data,
            e.datetime_gps,
            v.datetime_partida,
            v.datetime_chegada,
            e.datetime_captura,
            v.id_viagem,
            v.id_veiculo,
            v.indicador_ar_condicionado,
            e.id_validador,
            e.estado_equipamento,
            e.latitude,
            e.longitude,
            temperatura,
            extract(hour from datetime_gps) as hora
        from viagens as v
        left join
            gps_validador as e
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    gps_validador_indicadores as (  -- Indicadores de temperatura por veículo
        select
            data,
            id_viagem,
            id_validador,
            count(*) as quantidade_pre_tratamento,
            countif(temperatura is null) as quantidade_nula,
            countif(temperatura = 0) as quantidade_zero,
            countif(temperatura != 0) > 0 as indicador_temperatura_transmitida_viagem,
            count(distinct case when temperatura != 0 then temperatura end)
            > 1 as indicador_temperatura_variacao_viagem
        from gps_validador_viagem
        where indicador_ar_condicionado
        group by 1, 2, 3
    ),
    temperatura_inmet_alertario as (  -- Dados de temperatura externa
        select t.data, t.hora, t.temperatura
        from {{ ref("temperatura") }} as t
        where {{ incremental_filter }}
    ),
    metricas_base as (  -- 1 e 3 quartil da temperatura por hora e dia
        select
            g.id_viagem,
            g.id_validador,
            g.data,
            hora,
            temperatura,
            datetime_gps,
            percentile_cont(temperatura, 0.25) over (partition by g.data, hora) as q1,
            percentile_cont(temperatura, 0.75) over (partition by g.data, hora) as q3
        from gps_validador_viagem as g
        where temperatura is not null and temperatura != 0 and indicador_ar_condicionado
    ),
    metricas_iqr as (  -- IQR, limites inferior e superior
        select
            *,
            q3 - q1 as iqr,
            q1 - 1.5 * (q3 - q1) as iqr_limite_inferior,
            q3 + 1.5 * (q3 - q1) as iqr_limite_superior
        from metricas_base
    ),
    temperatura_filtrada_iqr as (  -- Filtro dos dados atípicos com IQR e calcula métricas base para Robust Z-Score
        select
            *,
            percentile_cont(temperatura, 0.5) over (partition by data, hora) as mediana
        from metricas_iqr
        where temperatura >= iqr_limite_inferior and temperatura <= iqr_limite_superior
    ),
    metrica_mediana as (  -- Métrica base para Robust Z-Score - Desvio Absoluto
        select *, abs(temperatura - mediana) as desvio_abs from temperatura_filtrada_iqr
    ),
    metrica_mad as (  -- Métrica base para Robust Z-Score - MAD
        select *, percentile_cont(desvio_abs, 0.5) over (partition by data, hora) as mad
        from metrica_mediana
    ),
    metrica_robust_z_score as (  -- Robust Z-Score
        select *, safe_divide(0.6745 * (temperatura - mediana), mad) as robust_z_score
        from metrica_mad
    ),
    temperatura_filtrada_total as (  -- Filtro dos dados atípicos com base no Robust Z-Score
        select * from metrica_robust_z_score where abs(robust_z_score) <= 3.5
    ),
    qtd_pos_tratamento as (  -- Calcula a quantidade de registros de temperatura após tratamento por viagem
        select
            data,
            id_viagem,
            id_validador,
            count(temperatura) as quantidade_pos_tratamento
        from temperatura_filtrada_total
        group by data, id_viagem, id_validador
    ),
    agg_temperatura_viagem as (  -- Percentuais de temperatura descartada e nula
        select
            data,
            id_viagem,
            id_validador,
            quantidade_pre_tratamento,
            quantidade_nula,
            quantidade_zero,
            coalesce(quantidade_pos_tratamento, 0) as quantidade_pos_tratamento,
            trunc(
                (
                    1 - coalesce(
                        safe_divide(
                            quantidade_pos_tratamento, quantidade_pre_tratamento
                        ),
                        0
                    )
                )
                * 100,
                2
            ) as percentual_temperatura_pos_tratamento_descartada,
            trunc(
                coalesce(safe_divide(quantidade_zero, quantidade_pre_tratamento), 0)
                * 100,
                2
            ) as percentual_temperatura_zero_descartada,
            trunc(
                coalesce(safe_divide(quantidade_nula, quantidade_pre_tratamento), 0)
                * 100,
                2
            ) as percentual_temperatura_nula_descartada,
            indicador_temperatura_transmitida_viagem,
            indicador_temperatura_variacao_viagem
        from gps_validador_indicadores
        left join qtd_pos_tratamento using (data, id_viagem, id_validador)
    ),
    classificacao_temperatura as (  -- Regras para classificação de temperatura regular
        select
            i.data,
            i.hora,
            i.id_veiculo,
            i.id_viagem,
            i.id_validador,
            i.indicador_ar_condicionado,
            i.datetime_gps,
            f.temperatura as temperatura_int,
            e.temperatura as temperatura_ext,
            f.temperatura <= 24
            or (
                (e.temperatura - f.temperatura) >= 8
            ) as classificacao_temperatura_regular,
            f.temperatura <= 24 as indicador_temperatura_menor_igual_24,
            (
                (e.temperatura - f.temperatura) >= 8
            ) as indicador_diferenca_temperatura_externa_interna
        from gps_validador_viagem as i
        left join
            temperatura_filtrada_total as f
            on f.data = i.data
            and f.id_viagem = i.id_viagem
            and f.datetime_gps = i.datetime_gps
            and f.id_validador = i.id_validador
        left join
            temperatura_inmet_alertario as e
            on e.data = extract(date from i.datetime_gps)
            and e.hora = extract(hour from i.datetime_gps)
    ),
    percentual_indicadores_validador_viagem as (  -- Indicadores de regularidade de temperatura por validador e viagem
        select
            data,
            id_veiculo,
            id_viagem,
            id_validador,
            case
                when max(indicador_ar_condicionado)
                then
                    trunc(
                        coalesce(
                            safe_divide(
                                countif(classificacao_temperatura_regular),
                                quantidade_pos_tratamento
                            ),
                            0
                        )
                        * 100,
                        2
                    )
                else null
            end as percentual_temperatura_regular,
            case
                when
                    max(indicador_ar_condicionado)
                    and percentual_temperatura_pos_tratamento_descartada = 100
                    and percentual_temperatura_zero_descartada < 100
                then true
                when max(indicador_ar_condicionado)
                then
                    (
                        coalesce(
                            safe_divide(
                                countif(classificacao_temperatura_regular),
                                quantidade_pos_tratamento
                            ),
                            0
                        )
                        * 100
                    )
                    >= 80
                else null
            end as indicador_temperatura_regular_viagem,
            case
                when max(indicador_ar_condicionado)
                then current_datetime("America/Sao_Paulo")
                else null
            end as datetime_verificacao_regularidade,
            indicador_temperatura_variacao_viagem,
            indicador_temperatura_transmitida_viagem,
            quantidade_pre_tratamento,
            quantidade_nula,
            quantidade_zero,
            quantidade_pos_tratamento,
            percentual_temperatura_nula_descartada,
            percentual_temperatura_zero_descartada,
            percentual_temperatura_pos_tratamento_descartada,
            percentual_temperatura_pos_tratamento_descartada
            > 50 as indicador_temperatura_pos_tratamento_descartada_viagem,
            percentual_temperatura_zero_descartada
            = 100 as indicador_temperatura_zero_viagem,
            percentual_temperatura_nula_descartada
            = 100 as indicador_temperatura_nula_viagem
        from classificacao_temperatura
        left join agg_temperatura_viagem using (data, id_viagem, id_validador)
        where id_validador is not null
        group by all
    ),
    indicador_regularidade as (  -- Calcula indicador_regularidade_temperatura para selecionar 1 validador
        select
            data,
            id_veiculo,
            id_viagem,
            id_validador,
            percentual_temperatura_regular,
            indicador_temperatura_regular_viagem,
            datetime_verificacao_regularidade,
            indicador_temperatura_variacao_viagem,
            indicador_temperatura_transmitida_viagem,
            quantidade_pre_tratamento,
            quantidade_nula,
            quantidade_zero,
            quantidade_pos_tratamento,
            percentual_temperatura_nula_descartada,
            percentual_temperatura_zero_descartada,
            percentual_temperatura_pos_tratamento_descartada,
            indicador_temperatura_pos_tratamento_descartada_viagem,
            indicador_temperatura_zero_viagem,
            indicador_temperatura_nula_viagem,
            (
                not indicador_temperatura_zero_viagem
                and indicador_temperatura_transmitida_viagem
                and indicador_temperatura_regular_viagem
            ) as indicador_regularidade_temperatura
        from percentual_indicadores_validador_viagem
    ),
    indicador_validador_agg as (  -- Agrega indicadores e percentuais por validador
        select
            ieb.data,
            ieb.id_viagem,
            array_agg(
                struct(
                    ieb.id_validador,
                    ieb.indicador_gps_servico_divergente,
                    ieb.indicador_estado_equipamento_aberto,
                    safe_cast(
                        ieb.percentual_estado_equipamento_aberto as string
                    ) as percentual_estado_equipamento_aberto,
                    indicador_temperatura_transmitida_viagem,
                    indicador_temperatura_variacao_viagem,
                    indicador_temperatura_nula_viagem,
                    safe_cast(
                        percentual_temperatura_nula_descartada as string
                    ) as percentual_temperatura_nula_descartada,
                    indicador_temperatura_zero_viagem,
                    safe_cast(
                        percentual_temperatura_zero_descartada as string
                    ) as percentual_temperatura_zero_descartada,
                    indicador_temperatura_pos_tratamento_descartada_viagem,
                    safe_cast(
                        percentual_temperatura_pos_tratamento_descartada as string
                    ) as percentual_temperatura_pos_tratamento_descartada,
                    percentual_temperatura_regular,
                    indicador_temperatura_regular_viagem,
                    quantidade_pre_tratamento,
                    quantidade_nula,
                    quantidade_zero,
                    quantidade_pos_tratamento
                )
                order by ieb.id_validador
            ) as valores
        from indicador_equipamento_bilhetagem ieb
        left join
            percentual_indicadores_validador_viagem piv using (
                data, id_viagem, id_validador
            )
        group by data, id_viagem
    ),
    /*
        Seleciona validador por:
        indicador_regularidade_temperatura desc,
        percentual_temperatura_regular desc,
        quantidade_pos_tratamento desc,
        indicador_temperatura_transmitida_viagem desc,
        id_validador asc
    */
    indicadores_validador as (
        select
            data,
            id_veiculo,
            id_viagem,
            id_validador,
            percentual_temperatura_regular,
            indicador_temperatura_regular_viagem,
            datetime_verificacao_regularidade,
            indicador_temperatura_variacao_viagem,
            indicador_temperatura_transmitida_viagem,
            quantidade_pre_tratamento,
            quantidade_nula,
            quantidade_zero,
            quantidade_pos_tratamento,
            percentual_temperatura_nula_descartada,
            percentual_temperatura_zero_descartada,
            percentual_temperatura_pos_tratamento_descartada,
            indicador_temperatura_pos_tratamento_descartada_viagem,
            indicador_temperatura_zero_viagem,
            indicador_temperatura_nula_viagem
        from indicador_regularidade
        qualify
            row_number() over (
                partition by data, id_viagem
                order by
                    indicador_regularidade_temperatura desc,
                    percentual_temperatura_regular desc,
                    quantidade_pos_tratamento desc,
                    indicador_temperatura_transmitida_viagem desc,
                    id_validador asc
            )
            = 1
    ),
    dados_novos as (  -- Estrutura indicadores em formato JSON sem datetime_verificacao_regularidade
        select
            v.data,
            v.id_viagem,
            iv.id_validador,
            v.id_veiculo,
            v.placa,
            v.ano_fabricacao,
            v.tipo_viagem,
            to_json_string(v.indicadores) as indicadores_str,
            v.datetime_partida,
            v.datetime_chegada,
            v.modo,
            v.servico,
            v.sentido,
            v.distancia_planejada,
            v.tecnologia_apurada,
            v.tecnologia_remunerada,
            iv.quantidade_pre_tratamento,
            iv.quantidade_nula,
            iv.quantidade_zero,
            iv.quantidade_pos_tratamento,
            to_json_string(
                struct(
                    struct(
                        iv.datetime_verificacao_regularidade,
                        iv.indicador_temperatura_regular_viagem as valor,
                        safe_cast(
                            iv.percentual_temperatura_regular as string
                        ) as percentual_temperatura_regular
                    ) as indicador_temperatura_regular_viagem,
                    struct(
                        iv.datetime_verificacao_regularidade,
                        iv.indicador_temperatura_variacao_viagem as valor
                    ) as indicador_temperatura_variacao_viagem,
                    struct(
                        iv.datetime_verificacao_regularidade,
                        iv.indicador_temperatura_transmitida_viagem as valor
                    ) as indicador_temperatura_transmitida_viagem,
                    struct(
                        iv.datetime_verificacao_regularidade,
                        iv.indicador_temperatura_pos_tratamento_descartada_viagem
                        as valor,
                        safe_cast(
                            iv.percentual_temperatura_pos_tratamento_descartada
                            as string
                        ) as percentual_temperatura_pos_tratamento_descartada
                    ) as indicador_temperatura_pos_tratamento_descartada_viagem,
                    struct(
                        iv.datetime_verificacao_regularidade,
                        iv.indicador_temperatura_nula_viagem as valor,
                        safe_cast(
                            iv.percentual_temperatura_nula_descartada as string
                        ) as percentual_temperatura_nula_descartada
                    ) as indicador_temperatura_nula_viagem,
                    struct(
                        iv.datetime_verificacao_regularidade,
                        iv.indicador_temperatura_zero_viagem as valor,
                        safe_cast(
                            iv.percentual_temperatura_zero_descartada as string
                        ) as percentual_temperatura_zero_descartada
                    ) as indicador_temperatura_zero_viagem,
                    struct(
                        iv.datetime_verificacao_regularidade, iva.valores
                    ) as indicador_validador
                )
            ) as indicadores_novos,
            0 as priority
        from viagens as v
        left join indicadores_validador iv using (data, id_viagem)
        left join indicador_validador_agg iva using (data, id_viagem)
    ),
    indicadores_concatenados as (  -- Concatena indicadores antes da comparação SHA
        select
            * except (indicadores_str, indicadores_novos),
            to_json_string(
                (
                    parse_json(
                        concat(
                            left(indicadores_str, length(indicadores_str) - 1),
                            ',',
                            substr(indicadores_novos, 2)
                        )
                    )
                )
            ) as indicadores_str
        from dados_novos
    ),
    {% if is_incremental() %}
        dados_atuais as (
            select
                * except (indicadores), to_json_string(indicadores) as indicadores_str,
            from {{ this }}
            {# from `rj-smtr`.`subsidio_staging`.`aux_viagem_temperatura` #}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} 1 = 0
                {% endif %}
        ),
    {% endif %}
    particoes_completas as (
        select *
        from indicadores_concatenados
        {% if is_incremental() %}
            union all by name

            select
                da.* except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais as da
            inner join indicadores_concatenados using (data, id_viagem)  -- Dados atuais só são incluídos se ainda existem nos dados novos
        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from particoes_completas
        qualify row_number() over (partition by data, id_viagem order by priority) = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                data,
                id_viagem,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais
        {% else %}
            select
                date(null) as data,
                cast(null as string) as id_viagem,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (data, id_viagem)
        from sha_dados_novos n
        left join sha_dados_atuais a using (data, id_viagem)
    ),
    struct_indicadores as (  -- Define datetime_verificacao_atual
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual,
                priority
            ),
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then current_datetime("America/Sao_Paulo")
                else datetime_ultima_atualizacao_atual
            end as datetime_verificacao_atual,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then '{{ invocation_id }}'
                else id_execucao_dbt_atual
            end as id_execucao_dbt
        from sha_dados_completos
    ),
    colunas_controle as (
        select
            data,
            id_viagem,
            id_veiculo,
            id_validador,
            placa,
            ano_fabricacao,
            datetime_partida,
            datetime_chegada,
            modo,
            tecnologia_apurada,
            tecnologia_remunerada,
            tipo_viagem,
            quantidade_pre_tratamento,
            quantidade_nula,
            quantidade_zero,
            quantidade_pos_tratamento,
            parse_json(
                regexp_replace(
                    indicadores_str,
                    r'"datetime_verificacao_regularidade":"[^"]*",',
                    concat(
                        '"datetime_verificacao_regularidade":"',
                        datetime_verificacao_atual,
                        '",'
                    )
                )
            ) as indicadores,
            servico,
            sentido,
            distancia_planejada,
            '{{ var("version") }}' as versao,
            datetime_verificacao_atual as datetime_ultima_atualizacao,
            id_execucao_dbt
        from struct_indicadores as s
    )
select *
from colunas_controle
