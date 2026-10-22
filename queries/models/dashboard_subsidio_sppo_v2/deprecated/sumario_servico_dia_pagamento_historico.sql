with
    pre_faixa_horaria as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            vista,
            viagens,
            km_apurada,
            km_planejada,
            perc_km_planejada,
            valor_subsidio_pago,
            valor_penalidade
        from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_historico`
        -- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
        where data < "2024-08-16"
    ),
    planejada as (
        select distinct data, consorcio, servico, vista
        from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
        -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where
            data >= "2024-08-16"
            and (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
            and format_time("%T", time(faixa_horaria_inicio)) != "00:00:00"
    ),
    pagamento as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            vista,
            viagens_dia as viagens,
            case
                when data >= "2024-09-01"
                then
                    coalesce(km_apurada_registrado_com_ar_inoperante, 0)
                    + coalesce(km_apurada_autuado_ar_inoperante, 0)
                    + coalesce(km_apurada_autuado_seguranca, 0)
                    + coalesce(km_apurada_autuado_limpezaequipamento, 0)
                    + coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
                    + coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
                    + coalesce(km_apurada_sem_transacao, 0)
                else
                    coalesce(km_apurada_registrado_com_ar_inoperante, 0)
                    + coalesce(km_apurada_autuado_ar_inoperante, 0)
                    + coalesce(km_apurada_autuado_seguranca, 0)
                    + coalesce(km_apurada_autuado_limpezaequipamento, 0)
                    + coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
                    + coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
                    + coalesce(km_apurada_sem_transacao, 0)
                    + coalesce(km_apurada_n_vistoriado, 0)
                    + coalesce(km_apurada_n_licenciado, 0)
            end as km_apurada,
            km_planejada_dia as km_planejada,
            valor_a_pagar as valor_subsidio_pago,
            valor_penalidade
        from `rj-smtr`.`dashboard_subsidio_sppo_v2`.`sumario_servico_dia_pagamento`
        -- `rj-smtr.dashboard_subsidio_sppo_v2.sumario_servico_dia_pagamento`
        left join planejada using (data, servico, consorcio)
        where data >= "2024-08-16"
    ),
    pos_faixa_horaria as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            vista,
            viagens,
            km_apurada,
            km_planejada,
            100 * km_apurada / km_planejada as perc_km_planejada,
            valor_subsidio_pago,
            valor_penalidade
        from pagamento
    )
select *
from pre_faixa_horaria
union all
select *
from pos_faixa_horaria
