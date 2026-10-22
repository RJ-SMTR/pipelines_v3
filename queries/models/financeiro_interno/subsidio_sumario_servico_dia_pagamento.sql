{% if var("start_date") >= var("DATA_SUBSIDIO_V14_INICIO") %}
    {{ config(enabled=false) }}
{% else %}
    {{
        config(
            materialized="incremental",
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
            incremental_strategy="insert_overwrite",
        )
    }}
{% endif %}

with
    subsidio_dia as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            sum(viagens_faixa) as viagens_dia,
            sum(km_planejada_faixa) as km_planejada_dia
        from {{ ref("percentual_operacao_faixa_horaria") }}
        -- from `rj-smtr.subsidio.percentual_operacao_faixa_horaria`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
        group by data, tipo_dia, consorcio, servico
    ),
    subsidio_parametros as (
        select distinct
            data_inicio,
            data_fim,
            status,
            subsidio_km,
            max(subsidio_km) over (
                partition by data_inicio, data_fim
            ) as subsidio_km_teto
        from {{ ref("valor_km_tipo_viagem") }}
    -- from `rj-smtr.subsidio.valor_km_tipo_viagem`
    ),
    penalidade as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            sum(valor_penalidade) as valor_penalidade
        from {{ ref("subsidio_penalidade_servico_faixa") }}
        -- from `rj-smtr.financeiro.subsidio_penalidade_servico_faixa`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
        group by data, tipo_dia, consorcio, servico
    ),
    subsidio_dia_tipo_viagem as (
        select *
        from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
        -- from `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
    ),
    valores_calculados as (
        select
            s.data,
            s.tipo_dia,
            s.consorcio,
            s.servico,
            pe.valor_penalidade,
            sum(s.km_apurada_faixa) as km_apurada_dia,
            sum(s.km_subsidiada_faixa) as km_subsidiada_dia,
            coalesce(sum(s.valor_acima_limite), 0) as valor_acima_limite,
            coalesce(sum(s.valor_total_sem_glosa), 0) as valor_total_sem_glosa,
            sum(s.valor_apurado) + pe.valor_penalidade as valor_total_com_glosa,
            case
                when pe.valor_penalidade != 0
                then - pe.valor_penalidade
                else
                    safe_cast(
                        (
                            sum(
                                if(
                                    indicador_viagem_dentro_limite = true
                                    and indicador_penalidade_judicial = true,
                                    km_apurada_faixa * subsidio_km_teto,
                                    0
                                )
                            ) - sum(
                                if(
                                    indicador_viagem_dentro_limite = true
                                    and indicador_penalidade_judicial = true,
                                    km_apurada_faixa * subsidio_km,
                                    0
                                )
                            )
                        ) as numeric
                    )
            end as valor_judicial,
        from subsidio_dia_tipo_viagem as s
        left join penalidade as pe using (data, tipo_dia, consorcio, servico)
        left join
            subsidio_parametros as sp
            on s.data between sp.data_inicio and sp.data_fim
            and s.tipo_viagem = sp.status
        group by s.data, s.tipo_dia, s.consorcio, s.servico, pe.valor_penalidade
    )
select
    sd.data,
    sd.tipo_dia,
    sd.consorcio,
    sd.servico,
    sd.viagens_dia,
    vc.km_apurada_dia,
    vc.km_subsidiada_dia,
    sd.km_planejada_dia,
    vc.valor_total_com_glosa as valor_a_pagar,
    vc.valor_total_com_glosa - vc.valor_total_sem_glosa as valor_glosado,
    vc.valor_acima_limite,
    vc.valor_total_sem_glosa,
    vc.valor_acima_limite
    + vc.valor_penalidade
    + vc.valor_total_sem_glosa as valor_total_apurado,
    vc.valor_judicial,
    vc.valor_penalidade,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from subsidio_dia as sd
left join valores_calculados as vc using (data, tipo_dia, consorcio, servico)
