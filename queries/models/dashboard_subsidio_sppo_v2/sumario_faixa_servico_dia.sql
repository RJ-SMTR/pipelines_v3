{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

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
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
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
            sum(valor_acima_limite) as valor_acima_limite,
            sum(valor_total_sem_glosa) as valor_total_sem_glosa
        from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
        -- from `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
        group by
            data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, consorcio, servico
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
                    tipo_viagem,
                    km_apurada_faixa
                from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
                -- from `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
                where
                    data between date('{{ var("start_date") }}') and date(
                        '{{ var("end_date") }}'
                    )
                    and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
            ) pivot (
                sum(km_apurada_faixa) as km_apurada for tipo_viagem in (
                    "Registrado com ar inoperante" as registrado_com_ar_inoperante,
                    "Não licenciado" as n_licenciado,
                    "Autuado por ar inoperante" as autuado_ar_inoperante,
                    "Autuado por segurança" as autuado_seguranca,
                    "Autuado por limpeza/equipamento" as autuado_limpezaequipamento,
                    "Licenciado sem ar e não autuado" as licenciado_sem_ar_n_autuado,
                    "Licenciado com ar e não autuado" as licenciado_com_ar_n_autuado,
                    "Não vistoriado" as n_vistoriado,
                    "Sem transação" as sem_transacao
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
    coalesce(
        km_apurada_registrado_com_ar_inoperante, 0
    ) as km_apurada_registrado_com_ar_inoperante,
    coalesce(km_apurada_n_licenciado, 0) as km_apurada_n_licenciado,
    coalesce(km_apurada_autuado_ar_inoperante, 0) as km_apurada_autuado_ar_inoperante,
    coalesce(km_apurada_autuado_seguranca, 0) as km_apurada_autuado_seguranca,
    coalesce(
        km_apurada_autuado_limpezaequipamento, 0
    ) as km_apurada_autuado_limpezaequipamento,
    coalesce(
        km_apurada_licenciado_sem_ar_n_autuado, 0
    ) as km_apurada_licenciado_sem_ar_n_autuado,
    coalesce(
        km_apurada_licenciado_com_ar_n_autuado, 0
    ) as km_apurada_licenciado_com_ar_n_autuado,
    coalesce(km_apurada_n_vistoriado, 0) as km_apurada_n_vistoriado,
    coalesce(km_apurada_sem_transacao, 0) as km_apurada_sem_transacao,
    agg.valor_apurado,
    agg.valor_acima_limite,
    agg.valor_total_sem_glosa,
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
where data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
