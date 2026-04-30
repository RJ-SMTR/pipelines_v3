{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    {% if var("flow_name") == "monitoramento_temperatura - materializacao" %}
        data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}") and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
    {% else %}
        data between date("{{var('start_date')}}") and date("{{var('end_date')}}") and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
    {% endif %}
{% endset %}

with
    viagens as (
        select
            data,
            servico_realizado as servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            id_viagem,
            distancia_planejada,
            sentido
        from {{ ref("viagem_completa") }}
        -- from `rj-smtr.projeto_subsidio_sppo.viagem_completa`
        where {{ incremental_filter }}
    ),
    veiculos as (
        select data, id_veiculo, placa, ano_fabricacao, tecnologia, status, indicadores
        from {{ ref("aux_veiculo_dia_consolidada") }}
        where {{ incremental_filter }}
    ),
    autuacao_disciplinar as (
        select data, datetime_autuacao, id_infracao, servico, placa
        from {{ ref("autuacao_disciplinar_historico") }}
        where
            (
                data_inclusao_datalake <= date_add(data, interval 7 day)
                or data_inclusao_datalake
                = date("{{var('data_inclusao_autuacao_disciplinar')}}")  -- Primeira data de inclusão dos dados de autuações disciplinares
            )
            and {{ incremental_filter }}
            and modo = "ONIBUS"
            and status != "Cancelada"
    ),
    ordem_status as (
        select distinct data_inicio, data_fim, status, ordem
        from {{ ref("valor_km_tipo_viagem") }}
    -- from `rj-smtr.subsidio.valor_km_tipo_viagem`
    ),
    tecnologias as (
        select
            inicio_vigencia,
            fim_vigencia,
            servico,
            codigo_tecnologia,
            maior_tecnologia_permitida,
            menor_tecnologia_permitida
        from {{ ref("tecnologia_servico") }}
    ),
    prioridade_tecnologia as (select * from {{ ref("tecnologia_prioridade") }}),
    veiculo_autuacao as (
        select
            ad.data,
            ad.datetime_autuacao,
            ad.id_infracao,
            ad.servico,
            ad.placa,
            ve.id_veiculo
        from autuacao_disciplinar ad
        left join veiculos ve on ad.data = ve.data and ad.placa = ve.placa
    ),
    viagem_status as (
        select
            v.data,
            v.servico,
            v.datetime_partida,
            v.datetime_chegada,
            v.id_veiculo,
            ve.placa,
            ve.ano_fabricacao,
            v.id_viagem,
            v.distancia_planejada,
            v.sentido,
            "Ônibus SPPO" as modo,
            ve.tecnologia,
            ve.status,
            ve.indicadores
        from viagens v
        left join veiculos ve on v.data = ve.data and v.id_veiculo = ve.id_veiculo
    ),
    viagem_tecnologia as (
        select
            vs.data,
            vs.servico,
            vs.datetime_partida,
            vs.datetime_chegada,
            vs.id_veiculo,
            vs.placa,
            vs.ano_fabricacao,
            vs.id_viagem,
            vs.distancia_planejada,
            vs.sentido,
            vs.modo,
            t.inicio_vigencia as data_inicio_vigencia,
            vs.tecnologia as tecnologia_apurada,
            case
                when p.prioridade > p_maior.prioridade
                then t.maior_tecnologia_permitida
                when
                    p.prioridade < p_menor.prioridade
                    and data >= date('{{ var("DATA_SUBSIDIO_V16_INICIO") }}')
                then null
                else vs.tecnologia
            end as tecnologia_remunerada,
            vs.status,
            vs.indicadores,
            case
                when p.prioridade < p_menor.prioridade then true else false
            end as indicador_penalidade_tecnologia
        from viagem_status vs
        left join
            tecnologias t
            on vs.servico = t.servico
            and (
                (vs.data between t.inicio_vigencia and t.fim_vigencia)
                or (vs.data >= t.inicio_vigencia and t.fim_vigencia is null)
            )
        left join prioridade_tecnologia as p on vs.tecnologia = p.tecnologia
        left join
            prioridade_tecnologia as p_maior
            on t.maior_tecnologia_permitida = p_maior.tecnologia
        left join
            prioridade_tecnologia as p_menor
            on t.menor_tecnologia_permitida = p_menor.tecnologia
    ),
    viagem_autuacao_hora as (
        select vt.data, vt.id_viagem, va.id_infracao
        from viagem_tecnologia vt
        left join
            veiculo_autuacao va
            on vt.data between va.data and date_add(va.data, interval 1 day)
            and vt.id_veiculo = va.id_veiculo
            and va.datetime_autuacao between vt.datetime_partida and vt.datetime_chegada
    ),
    viagem_classificada as (
        select
            vt.data,
            vt.id_viagem,
            vt.id_veiculo,
            vt.datetime_partida,
            vt.datetime_chegada,
            vt.modo,
            vt.placa,
            vt.ano_fabricacao,
            vt.tecnologia_apurada,
            vt.tecnologia_remunerada,
            case
                when vt.status = "Não licenciado"
                then "Não licenciado"
                when vt.status = "Não vistoriado"
                then "Não vistoriado"
                when vt.status = "Lacrado"
                then "Lacrado"
                when
                    vt.status = "Licenciado sem ar e não autuado"
                    and vt.servico
                    not in (select servico from {{ ref("servico_contrato_abreviado") }})
                    and vt.data >= date("{{ var('DATA_SUBSIDIO_V19_INICIO') }}")
                then "Não autorizado por ausência de ar-condicionado"
                when
                    vt.indicador_penalidade_tecnologia
                    and vt.data >= date('{{ var("DATA_SUBSIDIO_V16_INICIO") }}')
                then "Não autorizado por capacidade"
                when vt.status = "Autuado por ar inoperante"
                then "Autuado por ar inoperante"
                when vah.id_infracao = "017.III"
                then "Autuado por alterar itinerário"
                when vah.id_infracao = "023.X"
                then "Autuado por vista inoperante"
                when vah.id_infracao = "029.I"
                then "Autuado por não atender solicitação de parada"
                when vah.id_infracao = "029.XIII"
                then "Autuado por iluminação insuficiente"
                when vah.id_infracao = "040.I"
                then "Autuado por não concluir itinerário"
                when vt.status = "Registrado com ar inoperante"
                then "Registrado com ar inoperante"
                when vt.status = "Licenciado sem ar e não autuado"
                then "Licenciado sem ar e não autuado"
                when vt.status = "Licenciado com ar e não autuado"
                then "Licenciado com ar e não autuado"
            end as tipo_viagem,
            json_set(
                json_set(
                    indicadores,
                    '$.indicador_penalidade_tecnologia.valor',
                    vt.indicador_penalidade_tecnologia
                ),
                '$.indicador_penalidade_tecnologia.data_inicio_vigencia',
                vt.data_inicio_vigencia
            ) as indicadores,
            vt.servico,
            vt.sentido,
            vt.distancia_planejada,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            "{{ var('version') }}" as versao,
            '{{ invocation_id }}' as id_execucao_dbt
        from viagem_tecnologia vt
        left join
            viagem_autuacao_hora vah
            on vt.id_viagem = vah.id_viagem
            and vt.data = vah.data
    ),
    deduplica as (
        select
            data,
            id_viagem,
            id_veiculo,
            placa,
            ano_fabricacao,
            datetime_partida,
            datetime_chegada,
            modo,
            tecnologia_apurada,
            tecnologia_remunerada,
            tipo_viagem,
            indicadores,
            servico,
            sentido,
            distancia_planejada,
            datetime_ultima_atualizacao,
            versao,
            id_execucao_dbt
        from viagem_classificada vc
        left join
            ordem_status os
            on vc.data between os.data_inicio and os.data_fim
            and vc.tipo_viagem = os.status
        qualify
            row_number() over (partition by vc.data, vc.id_viagem order by os.ordem) = 1
    )
{% if is_incremental() %}
        ,
        dados_completos as (
            select *, 1 as ordem
            from deduplica

            union all

            select *, 0 as ordem
            from {{ this }}
            where {{ incremental_filter }}

        ),
        sha_dados as (
            {% set columns = (
                list_columns()
                | reject(
                    "in",
                    [
                        "versao",
                        "datetime_ultima_atualizacao",
                        "id_execucao_dbt",
                    ],
                )
                | list
            ) %}

            select
                *,
                sha256(
                    concat(
                        {% for c in columns %}
                            ifnull(
                                {% if c == "indicadores" %}to_json_string(indicadores)
                                {% else %}cast({{ c }} as string)
                                {% endif %},
                                'n/a'
                            )
                            {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                ) as sha_dado
            from dados_completos
        ),
        dados_completos_invocation_id as (
            select
                * except (id_execucao_dbt, sha_dado),
                case
                    when
                        lag(sha_dado) over (win) != sha_dado
                        or (lag(sha_dado) over (win) is null and ordem = 1)
                    then id_execucao_dbt
                    else lag(id_execucao_dbt) over (win)
                end as id_execucao_dbt
            from sha_dados
            window win as (partition by data, id_viagem order by ordem)
        )

    select * except (ordem)
    from dados_completos_invocation_id
    where ordem = 1
{% else %} select * from deduplica
{% endif %}
