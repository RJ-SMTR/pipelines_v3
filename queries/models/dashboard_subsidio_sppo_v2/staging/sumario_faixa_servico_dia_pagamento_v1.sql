{{ config(materialized="ephemeral") }}

{%- if execute -%}
    {%- set results = generate_km_columns() -%}
    {%- set tipos_viagem = results.columns[0].values() -%}
    {%- set colunas_tipo_viagem = results.columns[1].values() -%}

    {%- set tecnologias = ["MINI", "MIDI", "BASICO", "PADRON"] -%}
    {%- set tipos = [] -%}

    {%- for tipo, coluna in zip(tipos_viagem, colunas_tipo_viagem) -%}

        {%- set is_licenciado = tipo in [
            "Licenciado sem ar e não autuado",
            "Licenciado com ar e não autuado",
        ] -%}

        {%- set _ = tipos.append(
            {
                "nome": tipo,
                "coluna": coluna,
            }
        ) -%}

        {%- if is_licenciado -%}
            {%- for tech in tecnologias -%}
                {%- set _ = tipos.append(
                    {
                        "nome": tipo ~ " - " ~ tech,
                        "coluna": coluna ~ "_" ~ tech | lower,
                    }
                ) -%}
            {%- endfor -%}
        {%- endif -%}

    {%- endfor -%}
{%- endif -%}

{% set incremental_filter %}
    data between
        date('{{ var("start_date") }}')
        and date('{{ var("end_date") }}')
and data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
{% endset %}

with
    subsidio_faixa as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            viagens_faixa,
            km_planejada_faixa,
            pof
        from {{ ref("percentual_operacao_faixa_horaria") }}
        -- from `rj-smtr.subsidio.percentual_operacao_faixa_horaria`
        where {{ incremental_filter }}
    ),
    penalidade as (
        select
            data,
            tipo_dia,
            servico,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            valor_penalidade
        from {{ ref("subsidio_penalidade_servico_faixa") }}
        -- from `rj-smtr.financeiro.subsidio_penalidade_servico_faixa`
        where {{ incremental_filter }}
    ),
    subsidio_parametros as (
        select distinct
            data_inicio,
            data_fim,
            status,
            tecnologia,
            subsidio_km,
            case
                when tecnologia is null
                then
                    max(subsidio_km) over (
                        partition by date_trunc(data_inicio, year), data_fim
                    )
                when tecnologia is not null
                then
                    max(subsidio_km) over (
                        partition by date_trunc(data_inicio, year), data_fim, tecnologia
                    )
            end as subsidio_km_teto
        from {{ ref("valor_km_tipo_viagem") }}
    -- from `rj-smtr.subsidio.valor_km_tipo_viagem`
    ),
    subsidio_faixa_agg as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            sum(km_apurada_faixa) as km_apurada_faixa,
            sum(km_subsidiada_faixa) as km_subsidiada_faixa,
            sum(valor_apurado) as valor_apurado,
            sum(valor_glosado_tecnologia) as valor_glosado_tecnologia,
            sum(valor_acima_limite) as valor_acima_limite,
            sum(
                valor_total_sem_glosa - valor_glosado_tecnologia
            ) as valor_total_sem_glosa,
            sum(valor_apurado) + p.valor_penalidade as valor_total_com_glosa,
            case
                when
                    p.valor_penalidade != 0
                    and data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
                then - p.valor_penalidade
                else
                    safe_cast(
                        (
                            sum(
                                if(
                                    indicador_viagem_dentro_limite = true
                                    and indicador_penalidade_judicial = true,
                                    km_apurada_faixa * sp.subsidio_km_teto,
                                    0
                                )
                            ) - sum(
                                if(
                                    indicador_viagem_dentro_limite = true
                                    and indicador_penalidade_judicial = true,
                                    km_apurada_faixa * sp.subsidio_km,
                                    0
                                )
                            )
                        ) as numeric
                    )
            end as valor_judicial,
            p.valor_penalidade
        from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }} as s
        -- from `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem` as s
        left join
            penalidade as p using (
                data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, servico
            )
        left join
            subsidio_parametros as sp
            on s.data between sp.data_inicio and sp.data_fim
            and s.tipo_viagem = sp.status
            and (
                s.data >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
                or (
                    s.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and (
                        s.tecnologia_remunerada = sp.tecnologia
                        or (s.tecnologia_remunerada is null and sp.tecnologia is null)
                    )
                )
                or (
                    s.data < date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and sp.tecnologia is null
                )
            )
        where {{ incremental_filter }}
        group by
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            valor_penalidade
    ),
    pivot_data as (
        select *
        from
            (
                select
                    data,
                    tipo_dia,
                    faixa_horaria_inicio,
                    faixa_horaria_fim,
                    consorcio,
                    servico,
                    case
                        when
                            tipo_viagem in (
                                "Licenciado sem ar e não autuado",
                                "Licenciado com ar e não autuado"
                            )
                            and tecnologia_apurada is not null
                        then concat(tipo_viagem, ' - ', tecnologia_apurada)
                        else tipo_viagem
                    end as tipo_viagem_tecnologia,
                    km_apurada_faixa
                from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
                -- from `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
                where {{ incremental_filter }}
            ) pivot (
                sum(km_apurada_faixa) as km_apurada for tipo_viagem_tecnologia in (
                    {%- for tipo in tipos %}
                        "{{ tipo.nome }}" as {{ tipo.coluna }}
                        {%- if not loop.last %},{% endif %}
                    {%- endfor %}
                )
            )
    )
select
    s.data,
    s.tipo_dia,
    s.faixa_horaria_inicio,
    s.faixa_horaria_fim,
    s.consorcio,
    s.servico,
    s.viagens_faixa,
    agg.km_apurada_faixa,
    agg.km_subsidiada_faixa,
    s.km_planejada_faixa,
    s.pof,
    {%- for tipo in tipos %}
        coalesce(km_apurada_{{ tipo.coluna }}, 0) as km_apurada_{{ tipo.coluna }}
        {%- if not loop.last %},{% endif %}
    {%- endfor %},
    case
        when s.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
        then
            coalesce(km_apurada_licenciado_sem_ar_n_autuado_mini, 0)
            + coalesce(km_apurada_licenciado_sem_ar_n_autuado_midi, 0)
            + coalesce(km_apurada_licenciado_sem_ar_n_autuado_basico, 0)
            + coalesce(km_apurada_licenciado_sem_ar_n_autuado_padron, 0)
        else coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
    end as km_apurada_total_licenciado_sem_ar_n_autuado,
    case
        when s.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
        then
            coalesce(km_apurada_licenciado_com_ar_n_autuado_mini, 0)
            + coalesce(km_apurada_licenciado_com_ar_n_autuado_midi, 0)
            + coalesce(km_apurada_licenciado_com_ar_n_autuado_basico, 0)
            + coalesce(km_apurada_licenciado_com_ar_n_autuado_padron, 0)
        else coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
    end as km_apurada_total_licenciado_com_ar_n_autuado,
    agg.valor_total_com_glosa as valor_a_pagar,
    agg.valor_glosado_tecnologia,
    agg.valor_total_com_glosa - agg.valor_total_sem_glosa as valor_total_glosado,
    agg.valor_acima_limite,
    agg.valor_total_sem_glosa,
    agg.valor_acima_limite
    + agg.valor_penalidade
    + agg.valor_total_sem_glosa as valor_total_apurado,
    agg.valor_judicial,
    agg.valor_penalidade,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from subsidio_faixa as s
left join
    subsidio_faixa_agg as agg using (
        data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, consorcio, servico
    )
left join
    pivot_data as pd using (
        data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, consorcio, servico
    )
