{{
    config(
        materialized="incremental",
        alias="verificacao_guardador_veiculo",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set incremental_filter %}
    ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    and datetime_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

with
    movimentos as (
        select
            *,
            coalesce(
                id_area,
                concat("ESTACIONAMENTO-", cast(id_estacionamento_veiculo as string))
            ) as id_agrupamento_area
        from {{ ref("movimento_estacionamento_riorotativo") }}
        where placa_veiculo is not null
    ),
    periodos_ordenados as (
        select
            *,
            max(datetime_periodo_final) over (
                partition by placa_veiculo, id_agrupamento_area
                order by datetime_periodo_inicial, datetime_periodo_final
                rows between unbounded preceding and 1 preceding
            ) as datetime_fim_periodo_anterior
        from movimentos
    ),
    periodos_marcados as (
        select
            *,
            if(
                datetime_fim_periodo_anterior is null
                or datetime_periodo_inicial > datetime_fim_periodo_anterior,
                1,
                0
            ) as indicador_nova_janela
        from periodos_ordenados
    ),
    periodos_identificados as (
        select
            *,
            sum(indicador_nova_janela) over (
                partition by placa_veiculo, id_agrupamento_area
                order by datetime_periodo_inicial, datetime_periodo_final
            ) as numero_janela
        from periodos_marcados
    ),
    janelas_periodo_continuo as (
        select
            placa_veiculo,
            id_agrupamento_area,
            numero_janela,
            any_value(id_area) as id_area,
            any_value(ids_perfil_funcionamento) as ids_perfil_funcionamento,
            any_value(data_inicio_vigencia_area) as data_inicio_vigencia_area,
            any_value(data_fim_vigencia_area) as data_fim_vigencia_area,
            any_value(cpf_motorista) as cpf_motorista,
            min(datetime_periodo_inicial) as datetime_inicio_periodo,
            max(datetime_periodo_final) as datetime_fim_periodo,
            array_agg(
                id_movimento_estacionamento_veiculo order by datetime_periodo_inicial
            ) as ids_movimento_estacionamento
        from periodos_identificados
        group by placa_veiculo, id_agrupamento_area, numero_janela
    ),
    janelas_periodo as (
        select
            to_hex(
                sha256(
                    concat(
                        placa_veiculo,
                        "-",
                        id_agrupamento_area,
                        "-",
                        cast(
                            datetime_add(
                                datetime_inicio_periodo,
                                interval numero_subperiodo * 2 hour
                            ) as string
                        )
                    )
                )
            ) as id_janela_periodo,
            placa_veiculo,
            id_area,
            ids_perfil_funcionamento,
            data_inicio_vigencia_area,
            data_fim_vigencia_area,
            cpf_motorista,
            datetime_add(
                datetime_inicio_periodo, interval numero_subperiodo * 2 hour
            ) as datetime_inicio_periodo,
            least(
                datetime_add(
                    datetime_inicio_periodo, interval (numero_subperiodo + 1) * 2 hour
                ),
                datetime_fim_periodo
            ) as datetime_fim_periodo,
            numeric "2.00" as valor_pago_bruto,
            round(numeric "2.00" * numeric "0.042", 2) as valor_retido_jae,
            numeric "2.00"
            - round(numeric "2.00" * numeric "0.042", 2) as valor_pago_liquido,
            numeric "0" as valor_acrescimo,
            ids_movimento_estacionamento
        from janelas_periodo_continuo
        cross join
            unnest(
                generate_array(
                    0,
                    greatest(
                        div(
                            datetime_diff(
                                datetime_fim_periodo, datetime_inicio_periodo, minute
                            )
                            + 119,
                            120
                        )
                        - 1,
                        0
                    )
                )
            ) as numero_subperiodo
    ),
    fiscalizacao_staging as (
        select *
        from {{ ref("staging_fiscalizacao_veiculo_riorotativo") }}
        where {{ incremental_filter }}
    ),
    fiscalizacao_imagem as (
        select
            id_fiscalizacao_veiculo,
            indicador_imagem_placa,
            indicador_imagem1_veiculo,
            indicador_imagem2_veiculo,
            datetime_inclusao_imagem,
        from {{ ref("staging_fiscalizacao_veiculo_imagem_riorotativo") }}
        where {{ incremental_filter }}
    ),
    veiculo as (
        select *
        from {{ ref("staging_veiculo_riorotativo") }}
        where {{ incremental_filter }}
    ),
    fiscalizacao as (
        select
            f.*,
            coalesce(
                nullif(f.placa_digitada, ""), f.placa_ocr, v.placa
            ) as placa_veiculo,
            i.datetime_inclusao_imagem,
            ifnull(i.indicador_imagem_placa, false) as indicador_imagem_placa,
            ifnull(i.indicador_imagem1_veiculo, false) as indicador_imagem1_veiculo,
            ifnull(i.indicador_imagem2_veiculo, false) as indicador_imagem2_veiculo
        from fiscalizacao_staging as f
        left join veiculo as v using (id_veiculo)
        left join fiscalizacao_imagem as i using (id_fiscalizacao_veiculo)
        where
            date(
                f.datetime_fiscalizacao
            ) between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
            and date(f.datetime_inclusao)
            <= date_add(date(f.datetime_fiscalizacao), interval 1 day)
    ),
    fiscalizacao_periodo as (
        select
            f.*,
            j.id_janela_periodo,
            j.id_area,
            j.ids_perfil_funcionamento,
            j.data_inicio_vigencia_area,
            j.data_fim_vigencia_area,
            j.cpf_motorista,
            j.datetime_inicio_periodo,
            j.datetime_fim_periodo,
            j.valor_pago_bruto,
            j.valor_retido_jae,
            j.valor_pago_liquido,
            j.valor_acrescimo,
            j.ids_movimento_estacionamento,
            row_number() over (
                partition by f.id_fiscalizacao_veiculo
                order by j.datetime_inicio_periodo desc, j.id_area
            ) as ordem_periodo
        from fiscalizacao as f
        left join
            janelas_periodo as j
            on f.placa_veiculo = j.placa_veiculo
            and f.datetime_fiscalizacao
            between j.datetime_inicio_periodo and j.datetime_fim_periodo
    ),
    fiscalizacao_periodo_unico as (
        select * except (ordem_periodo)
        from fiscalizacao_periodo
        where ordem_periodo = 1
    ),
    perfil_funcionamento_candidato as (
        select
            f.id_fiscalizacao_veiculo,
            p.perfil_funcionamento_codigo,
            p.perfil_funcionamento_dia_semana,
            p.perfil_funcionamento_horario_inicio,
            p.perfil_funcionamento_horario_fim,
            row_number() over (
                partition by f.id_fiscalizacao_veiculo, p.perfil_funcionamento_codigo
                order by p.data desc
            ) as ordem_perfil
        from fiscalizacao_periodo_unico as f
        left join
            {{ ref("staging_perfil_funcionamento_riorotativo") }} as p
            on p.perfil_funcionamento_codigo
            in unnest(ifnull(f.ids_perfil_funcionamento, cast([] as array<string>)))
            and p.data <= date(f.datetime_fiscalizacao)
    ),
    perfil_funcionamento_vigente as (
        select * except (ordem_perfil)
        from perfil_funcionamento_candidato
        where ordem_perfil = 1
    ),
    perfil_funcionamento_atividade as (
        select
            f.id_fiscalizacao_veiculo,
            countif(
                extract(dayofweek from f.datetime_fiscalizacao)
                in unnest(ifnull(p.perfil_funcionamento_dia_semana, []))
                and case
                    when
                        p.perfil_funcionamento_horario_inicio
                        <= p.perfil_funcionamento_horario_fim
                    then
                        time(f.datetime_fiscalizacao)
                        between p.perfil_funcionamento_horario_inicio
                        and p.perfil_funcionamento_horario_fim
                    else
                        time(f.datetime_fiscalizacao)
                        >= p.perfil_funcionamento_horario_inicio
                        or time(f.datetime_fiscalizacao)
                        <= p.perfil_funcionamento_horario_fim
                end
            )
            > 0 as indicador_vaga_perfil_funcionamento_ativo
        from fiscalizacao_periodo_unico as f
        left join perfil_funcionamento_vigente as p using (id_fiscalizacao_veiculo)
        group by f.id_fiscalizacao_veiculo
    ),
    fiscalizacao_periodo_perfil as (
        select
            f.*,
            ifnull(
                f.id_janela_periodo is not null
                and date(f.datetime_fiscalizacao) >= f.data_inicio_vigencia_area
                and (
                    date(f.datetime_fiscalizacao) <= f.data_fim_vigencia_area
                    or f.data_fim_vigencia_area is null
                ),
                false
            ) as indicador_vaga_vigente,
            ifnull(
                p.indicador_vaga_perfil_funcionamento_ativo, false
            ) as indicador_vaga_perfil_funcionamento_ativo
        from fiscalizacao_periodo_unico as f
        left join perfil_funcionamento_atividade as p using (id_fiscalizacao_veiculo)
    ),
    guardador_entidade as (
        select
            data,
            documento as cpf_guardador_veiculo,
            array_agg(distinct cnpj ignore nulls) as cnpjs_entidade,
            array_agg(distinct status ignore nulls) as status_guardador
        from {{ ref("guardador_veiculo_riorotativo_historico") }}
        group by data, cpf_guardador_veiculo
    ),
    fiscalizacao_entidade as (
        select
            f.*,
            ifnull(g.cnpjs_entidade, cast([] as array<string>)) as cnpjs_entidade,
            ifnull(g.status_guardador, cast([] as array<string>)) as status_guardador,
            coalesce(
                f.id_janela_periodo,
                concat(
                    "SEM_PERIODO-",
                    ifnull(f.placa_veiculo, cast(f.id_fiscalizacao_veiculo as string)),
                    "-",
                    cast(date(f.datetime_fiscalizacao) as string)
                )
            ) as id_janela_verificacao
        from fiscalizacao_periodo_perfil as f
        left join
            guardador_entidade as g
            on date(f.datetime_fiscalizacao) = g.data
            and f.cpf_guardador_veiculo = g.cpf_guardador_veiculo
    ),
    fiscalizacao_ordenada as (
        select
            *,
            row_number() over (
                partition by id_janela_verificacao
                order by datetime_fiscalizacao, id_fiscalizacao_veiculo
            ) as ordem_verificacao_janela,
            min(datetime_inclusao_imagem) over (
                partition by id_janela_verificacao
            ) as datetime_primeira_imagem_janela
        from fiscalizacao_entidade
    ),
    fiscalizacao_motivo as (
        select
            *,
            case
                when id_janela_periodo is not null and not indicador_vaga_vigente
                then "VAGA_FORA_VIGENCIA"
                when
                    id_janela_periodo is not null
                    and not indicador_vaga_perfil_funcionamento_ativo
                then "VAGA_FORA_FUNCIONAMENTO"
                when
                    datetime_primeira_imagem_janela is not null
                    and datetime_inclusao_imagem > datetime_primeira_imagem_janela
                then "FOTOGRAFIA_POSTERIOR_PRIMEIRA_VALIDACAO"
                when id_janela_periodo is null
                then "AUSENCIA_PERIODO_TARIFADO"
                when ordem_verificacao_janela > 1
                then "OCORRENCIA_DUPLICADA"
            end as motivo_nao_repasse
        from fiscalizacao_ordenada
    ),
    fiscalizacao_classificada as (
        select
            * replace (
                if(
                    ordem_verificacao_janela = 1,
                    ifnull(valor_pago_bruto, numeric "0"),
                    numeric "0"
                ) as valor_pago_bruto,
                if(
                    ordem_verificacao_janela = 1,
                    ifnull(valor_retido_jae, numeric "0"),
                    numeric "0"
                ) as valor_retido_jae,
                if(
                    ordem_verificacao_janela = 1,
                    ifnull(valor_pago_liquido, numeric "0"),
                    numeric "0"
                ) as valor_pago_liquido,
                if(
                    ordem_verificacao_janela = 1,
                    ifnull(valor_acrescimo, numeric "0"),
                    numeric "0"
                ) as valor_acrescimo
            ),
            motivo_nao_repasse is null as indicador_verificacao_valida
        from fiscalizacao_motivo
    )
select
    date(datetime_fiscalizacao) as data,
    cast(id_fiscalizacao_veiculo as string) as id_verificacao,
    datetime_fiscalizacao as datetime_verificacao,
    datetime_inclusao as datetime_inclusao_verificacao,
    datetime_analise,
    datetime_inclusao_imagem,
    datetime_primeira_imagem_janela,
    datetime_inicio_periodo,
    datetime_fim_periodo,
    cpf_guardador_veiculo,
    cpf_motorista,
    placa_veiculo,
    placa_ocr,
    placa_digitada,
    id_veiculo,
    id_area,
    latitude,
    longitude,
    indicador_imagem_placa,
    indicador_imagem1_veiculo,
    indicador_imagem2_veiculo,
    indicador_vaga_vigente,
    indicador_vaga_perfil_funcionamento_ativo,
    cnpjs_entidade,
    status_guardador,
    ids_movimento_estacionamento,
    indicador_verificacao_valida,
    motivo_nao_repasse,
    ifnull(valor_pago_bruto, numeric "0") as valor_pago_bruto,
    ifnull(valor_retido_jae, numeric "0") as valor_retido_jae,
    ifnull(valor_pago_liquido, numeric "0") as valor_pago_liquido,
    ifnull(valor_acrescimo, numeric "0") as valor_acrescimo,
    if(
        indicador_verificacao_valida, numeric "1.40", numeric "0"
    ) as valor_repasse_guardador_veiculo,
    if(
        indicador_verificacao_valida, numeric "0.11", numeric "0"
    ) as valor_repasse_entidade_a,
    if(
        indicador_verificacao_valida, numeric "0.11", numeric "0"
    ) as valor_repasse_entidade_b,
    if(
        indicador_verificacao_valida, numeric "0.11", numeric "0"
    ) as valor_repasse_entidade_unitario,
    if(
        indicador_verificacao_valida,
        valor_pago_bruto
        - valor_retido_jae
        - numeric "1.40"
        - numeric "0.11"
        - numeric "0.11",
        valor_pago_bruto - valor_retido_jae
    ) as valor_repasse_pcrj,
    valor_retido_jae as valor_repasse_concessionaria,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from fiscalizacao_classificada
