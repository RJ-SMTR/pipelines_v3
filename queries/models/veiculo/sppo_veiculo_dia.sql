{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

{% if is_incremental() and execute %}
    {% set licenciamento_dates = run_query(
        get_version_dates("licenciamento_data_versao_efetiva")
    ) %}
    {% set min_licenciamento_date = licenciamento_dates.columns[0].values()[0] %}
    {% set max_licenciamento_date = licenciamento_dates.columns[1].values()[0] %}

    {% set infracao_dates = run_query(
        get_version_dates("infracao_data_versao_efetiva")
    ) %}
    {% set min_infracao_date = infracao_dates.columns[0].values()[0] %}
    {% set max_infracao_date = infracao_dates.columns[1].values()[0] %}
{% endif %}
with
    licenciamento_data_versao as (
        select *
        from {{ ref("licenciamento_data_versao_efetiva") }}
        {% if is_incremental() %}
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        {% endif %}
    ),
    infracao_data_versao as (
        select *
        from {{ ref("infracao_data_versao_efetiva") }}
        {% if is_incremental() %}
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        {% endif %}
    ),
    licenciamento as (
        select
            date(dve.data) as data,
            id_veiculo,
            placa,
            tipo_veiculo,
            tecnologia,
            indicador_ar_condicionado,
            true as indicador_licenciado,
            case
                when
                    ano_ultima_vistoria_atualizado >= cast(
                        extract(
                            year
                            from
                                date_sub(
                                    date(dve.data),
                                    interval {{
                                        var(
                                            "sppo_licenciamento_validade_vistoria_ano"
                                        )
                                    }} year
                                )
                        ) as int64
                    )
                then true  -- Última vistoria realizada dentro do período válido
                when
                    data_ultima_vistoria is null
                    and date_diff(date(dve.data), data_inicio_vinculo, day)
                    <= {{ var("sppo_licenciamento_tolerancia_primeira_vistoria_dia") }}
                then true  -- Caso o veículo seja novo, existe a tolerância de 15 dias para a primeira vistoria
                when
                    ano_fabricacao in (2023, 2024)
                    and cast(extract(year from date(dve.data)) as int64) = 2024
                then true  -- Caso o veículo tiver ano de fabricação 2023 ou 2024, será considerado como vistoriado apenas em 2024 (regra de transição)
                else false
            end as indicador_vistoriado,
            data_inicio_vinculo
        from {{ ref("sppo_licenciamento") }} l
        right join licenciamento_data_versao as dve on l.data = dve.data_versao
        {% if is_incremental() %}
            where
                dve.data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
                and l.data
                between "{{ min_licenciamento_date }}"
                and "{{ max_licenciamento_date }}"
        {% endif %}
    ),
    gps as (
        select data, id_veiculo
        from {{ ref("gps_sppo") }}
        -- `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
        where
            {% if is_incremental() %}
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
            {% else %}  -- data >= "2023-01-16"
            {% endif %}
        group by 1, 2
    ),
    autuacoes as (
        select distinct data_infracao as data, placa, id_infracao
        from {{ ref("sppo_infracao") }} i
        -- `rj-smtr.veiculo.sppo_infracao` i
        right join
            infracao_data_versao as dve
            on i.data = dve.data_versao
            and data_infracao = date(dve.data)
        {% if is_incremental() %}
            where
                i.data between date("{{ min_infracao_date }}") and date(
                    "{{ max_infracao_date }}"
                )
                and data_infracao between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        {% endif %}
    ),
    registros_agente_verao as (
        select distinct
            data, id_veiculo, true as indicador_registro_agente_verao_ar_condicionado
        from {{ ref("sppo_registro_agente_verao") }}
        -- `rj-smtr.veiculo.sppo_registro_agente_verao`
        right join infracao_data_versao dve using (data)
        {% if is_incremental() %}
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        {% endif %}
    ),
    autuacao_ar_condicionado as (
        select data, placa, true as indicador_autuacao_ar_condicionado
        from autuacoes
        where id_infracao = "023.II"
    ),
    autuacao_seguranca as (
        select data, placa, true as indicador_autuacao_seguranca
        from autuacoes
        where
            id_infracao in (
                "016.VI",
                "023.VII",
                "024.II",
                "024.III",
                "024.IV",
                "024.V",
                "024.VI",
                "024.VII",
                "024.VIII",
                "024.IX",
                "024.XII",
                "024.XIV",
                "024.XV",
                "025.II",
                "025.XII",
                "025.XIII",
                "025.XIV",
                "026.X"
            )
    ),
    autuacao_equipamento as (
        select data, placa, true as indicador_autuacao_equipamento
        from autuacoes
        where
            id_infracao in (
                "023.IV",
                "023.V",
                "023.VI",
                "023.VIII",
                "024.XIII",
                "024.XI",
                "024.XVIII",
                "024.XXI",
                "025.III",
                "025.IV",
                "025.V",
                "025.VI",
                "025.VII",
                "025.VIII",
                "025.IX",
                "025.X",
                "025.XI"
            )
    ),
    autuacao_limpeza as (
        select data, placa, true as indicador_autuacao_limpeza
        from autuacoes
        where id_infracao in ("023.IX", "024.X")
    ),
    autuacoes_agg as (
        select distinct *
        from autuacao_ar_condicionado
        full join autuacao_seguranca using (data, placa)
        full join autuacao_equipamento using (data, placa)
        full join autuacao_limpeza using (data, placa)
    ),
    gps_licenciamento_autuacao as (
        select
            data,
            id_veiculo,
            struct(
                coalesce(l.indicador_licenciado, false) as indicador_licenciado,
                if(
                    data >= "{{ var('DATA_SUBSIDIO_V5_INICIO') }}",
                    coalesce(l.indicador_vistoriado, false),
                    null
                ) as indicador_vistoriado,
                coalesce(
                    l.indicador_ar_condicionado, false
                ) as indicador_ar_condicionado,
                coalesce(
                    a.indicador_autuacao_ar_condicionado, false
                ) as indicador_autuacao_ar_condicionado,
                coalesce(
                    a.indicador_autuacao_seguranca, false
                ) as indicador_autuacao_seguranca,
                coalesce(
                    a.indicador_autuacao_limpeza, false
                ) as indicador_autuacao_limpeza,
                coalesce(
                    a.indicador_autuacao_equipamento, false
                ) as indicador_autuacao_equipamento,
                coalesce(
                    r.indicador_registro_agente_verao_ar_condicionado, false
                ) as indicador_registro_agente_verao_ar_condicionado
            ) as indicadores,
            l.placa,
            tecnologia
        from gps g
        left join licenciamento as l using (data, id_veiculo)
        left join autuacoes_agg as a using (data, placa)
        left join registros_agente_verao as r using (data, id_veiculo)
    ),
    sppo_veiculo_dia_final as (
        select
            gla.* except (indicadores, tecnologia, placa),
            to_json(indicadores) as indicadores,
            case
                {% if not is_incremental() or var("start_date") < var(
                    "DATA_SUBSIDIO_V5_INICIO"
                ) %}
                    when data < date("{{ var('DATA_SUBSIDIO_V5_INICIO') }}") then status
                {% endif %}
                when data >= date("{{ var('DATA_SUBSIDIO_V5_INICIO') }}")
                then
                    case
                        when indicadores.indicador_licenciado is false
                        then "Não licenciado"
                        when indicadores.indicador_vistoriado is false
                        then "Não vistoriado"
                        when
                            indicadores.indicador_ar_condicionado is true
                            and indicadores.indicador_autuacao_ar_condicionado is true
                        then "Autuado por ar inoperante"
                        when
                            indicadores.indicador_ar_condicionado is true
                            and indicadores.indicador_registro_agente_verao_ar_condicionado
                            is true
                        then "Registrado com ar inoperante"
                        when
                            data < date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
                            and indicadores.indicador_autuacao_seguranca is true
                        then "Autuado por segurança"
                        when
                            data < date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
                            and indicadores.indicador_autuacao_limpeza is true
                            and indicadores.indicador_autuacao_equipamento is true
                        then "Autuado por limpeza/equipamento"
                        when indicadores.indicador_ar_condicionado is false
                        then "Licenciado sem ar e não autuado"
                        when indicadores.indicador_ar_condicionado is true
                        then "Licenciado com ar e não autuado"
                        else null
                    end
            end as status,
            if(
                data >= date("{{ var('DATA_SUBSIDIO_V13_INICIO') }}"),
                tecnologia,
                safe_cast(null as string)
            ) as tecnologia,

            if(
                data >= date("{{ var('DATA_SUBSIDIO_V13_INICIO') }}"),
                placa,
                safe_cast(null as string)
            ) as placa,

            if(
                data >= date("{{ var('DATA_SUBSIDIO_V13_INICIO') }}"),
                date(l.data_versao),
                safe_cast(null as date)
            ) as data_licenciamento,

            if(
                data >= date("{{ var('DATA_SUBSIDIO_V13_INICIO') }}"),
                date(i.data_versao),
                safe_cast(null as date)
            ) as data_infracao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            "{{ var('version') }}" as versao
        from gps_licenciamento_autuacao as gla
        left join licenciamento_data_versao as l using (data)
        left join infracao_data_versao as i using (data)
        {% if not is_incremental() or var("start_date") < var(
                "DATA_SUBSIDIO_V5_INICIO"
            ) %}
            left join
                {{ ref("subsidio_parametros") }} as p
                -- `rj-smtr.dashboard_subsidio_sppo.subsidio_parametros` as p
                on gla.indicadores.indicador_licenciado = p.indicador_licenciado
                and gla.indicadores.indicador_ar_condicionado
                = p.indicador_ar_condicionado
                and gla.indicadores.indicador_autuacao_ar_condicionado
                = p.indicador_autuacao_ar_condicionado
                and gla.indicadores.indicador_autuacao_seguranca
                = p.indicador_autuacao_seguranca
                and gla.indicadores.indicador_autuacao_limpeza
                = p.indicador_autuacao_limpeza
                and gla.indicadores.indicador_autuacao_equipamento
                = p.indicador_autuacao_equipamento
                and gla.indicadores.indicador_registro_agente_verao_ar_condicionado
                = p.indicador_registro_agente_verao_ar_condicionado
                and (data between p.data_inicio and p.data_fim)
                and data < date("{{ var('DATA_SUBSIDIO_V5_INICIO')}}")
        {% endif %}
        {% if is_incremental() %}
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        {% endif %}
    )
select *
from sppo_veiculo_dia_final
where data <= '{{ var("data_final_veiculo_arquitetura_1") }}'
