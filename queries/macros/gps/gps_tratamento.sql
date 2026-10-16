{% macro gps_tratamento(staging_gps, aux_gps_filtrada) %}
    {% set incremental_filter %}
        data between
            date('{{ var("date_range_start") }}')
            and date('{{ var("date_range_end") }}')
    {% endset %}
    with
        registros as (
            select
                data,
                hora,
                id_veiculo,
                datetime_gps,
                datetime_captura,
                velocidade,
                servico,
                latitude,
                longitude,
            from {{ aux_gps_filtrada }}
            where
                {{ incremental_filter }}
                and datetime_gps
                between datetime('{{ var("date_range_start") }}') and datetime(
                    '{{ var("date_range_end") }}'
                )
        ),
        velocidades as (
            select
                id_veiculo,
                datetime_gps,
                servico,
                velocidade,
                distancia,
                indicador_em_movimento
            from ({{ gps_aux_velocidade(aux_gps_filtrada) }})
        ),
        paradas as (
            select id_veiculo, datetime_gps, servico, tipo_parada
            from ({{ gps_aux_parada(aux_gps_filtrada) }})
        ),
        indicadores as (
            select id_veiculo, datetime_gps, servico, indicador_trajeto_correto
            from ({{ gps_aux_trajeto_correto(aux_gps_filtrada) }})
        ),
        novos_dados as (
            select
                date(r.datetime_gps) as data,
                extract(hour from r.datetime_gps) as hora,
                r.datetime_gps,
                r.id_veiculo,
                r.servico,
                r.latitude,
                r.longitude,
                case
                    when
                        indicador_em_movimento is true
                        and indicador_trajeto_correto is true
                    then 'Em operação'
                    when
                        indicador_em_movimento is true
                        and indicador_trajeto_correto is false
                    then 'Operando fora trajeto'
                    when indicador_em_movimento is false
                    then
                        case
                            when tipo_parada is not null
                            then concat("Parado ", tipo_parada)
                            when indicador_trajeto_correto is true
                            then 'Parado trajeto correto'
                            else 'Parado fora trajeto'
                        end
                end as status,
                r.velocidade as velocidade_instantanea,
                v.velocidade as velocidade_estimada_10_min,
                v.distancia,
                0 as priority
            from registros r
            join indicadores i using (id_veiculo, datetime_gps, servico)
            join velocidades v using (id_veiculo, datetime_gps, servico)
            join paradas p using (id_veiculo, datetime_gps, servico)
        ),
        particoes_completas as (
            select *
            from novos_dados

            {% if is_incremental() %}
                union all

                select * except (versao, datetime_ultima_atualizacao), 1 as priority
                from {{ this }}
                where {{ incremental_filter }}
            {% endif %}
        )
    select
        * except (priority),
        '{{ var("version") }}' as versao,
        current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
    from particoes_completas
    qualify
        row_number() over (
            partition by id_veiculo, datetime_gps, servico order by priority
        )
        = 1
{% endmacro %}
