{% if var("encontro_contas_modo") == "_pos_gt" %}
    -- 0. Lista servicos e dias atípicos (pagos por recurso)
    with
        recursos as (
            select
                data,
                id_recurso,
                tipo_recurso,
                -- consorcio,
                servico,
                sum(valor_pago) as valor_pago
            from {{ ref("recursos_sppo_servico_dia_pago") }}
            -- `rj-smtr`.`br_rj_riodejaneiro_recursos`.`recursos_sppo_servico_dia_pago`
            group by 1, 2, 3, 4
        ),
        servico_dia_atipico as (
            select distinct data, servico
            from recursos
            where
                -- Quando o valor do recurso pago for R$ 0, desconsidera-se o recurso,
                -- pois:
                -- Recurso pode ter sido cancelado (pago e depois revertido)
                -- Problema reporto não gerou impacto na operação (quando aparece
                -- apenas 1 vez)
                valor_pago != 0
                -- Desconsideram-se recursos do tipo "Algoritmo" (igual a apuração em
                -- produção, levantado pela TR/SUBTT/CMO)
                -- Desconsideram-se recursos do tipo "Viagem Individual" (não afeta
                -- serviço-dia)
                and tipo_recurso not in ("Algoritmo", "Viagem Individual")
                -- Desconsideram-se recursos de reprocessamento que já constam em
                -- produção
                and not (
                    data between "2022-06-01" and "2022-06-30"
                    and tipo_recurso = "Reprocessamento"
                )
        ),

        -- 1. Calcula a km subsidiada por servico e dia
        sumario_dia as (  -- Km apurada por servico e dia
            select
                data,
                consorcio,
                servico,
                sum(km_apurada) as km_subsidiada,
                sum(valor_subsidio_pago) as subsidio_pago
            from {{ ref("sumario_servico_dia_historico") }}
            -- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
            where data between "2022-06-01" and "2023-12-31" and valor_subsidio_pago > 0
            group by 1, 2, 3
        ),
        viagem_remunerada as (  -- Km subsidiada pos regra do teto de 120% por servico e dia
            select data, servico, sum(distancia_planejada) as km_subsidiada
            from {{ ref("viagens_remuneradas") }}
            -- `rj-smtr.dashboard_subsidio_sppo.viagens_remuneradas`
            where
                data between "2023-09-16" and "2023-12-31"
                and indicador_viagem_dentro_limite = true  -- useless
            group by 1, 2
        ),
        km_subsidiada_dia as (
            select
                sd.* except (km_subsidiada),
                ifnull(
                    case
                        when data >= "2023-09-16"
                        then vr.km_subsidiada
                        else sd.km_subsidiada
                    end,
                    0
                ) as km_subsidiada
            from sumario_dia sd
            left join viagem_remunerada as vr using (data, servico)
        ),

        -- 2. Filtra km subsidiada apenas em dias típicos (remove servicos e dias
        -- pagos por recurso)
        km_subsidiada_filtrada as (
            select ksd.*
            from km_subsidiada_dia ksd
            left join servico_dia_atipico sda using (data, servico)
            where
                sda.data is null
                -- Demais dias que não foi considerada a km apurada via GPS:
                and ksd.data not in (
                    "2022-10-02",
                    "2022-10-30",
                    '2023-02-07',
                    '2023-02-08',
                    '2023-02-10',
                    '2023-02-13',
                    '2023-02-17',
                    '2023-02-18',
                    '2023-02-19',
                    '2023-02-20',
                    '2023-02-21',
                    '2023-02-22'
                )
        ),

        -- 3. Calcula a receita tarifária por servico e dia
        rdo as (
            select
                data,
                consorcio,
                case
                    when length(linha) < 3
                    then lpad(linha, 3, "0")
                    else
                        concat(
                            ifnull(regexp_extract(linha, r"[A-Z]+"), ""),
                            ifnull(regexp_extract(linha, r"[0-9]+"), "")
                        )
                end as servico,
                sum(receita_buc)
                + sum(receita_buc_supervia)
                + sum(receita_cartoes_perna_unica_e_demais)
                + sum(receita_especie) as receita_tarifaria_aferida
            from {{ ref("rdo40_registros") }}
            -- `rj-smtr`.`br_rj_riodejaneiro_rdo`.`rdo40_registros`
            where
                data between "2022-06-01" and "2023-12-31"
                and data not in (
                    "2022-10-02",
                    "2022-10-30",
                    '2023-02-07',
                    '2023-02-08',
                    '2023-02-10',
                    '2023-02-13',
                    '2023-02-17',
                    '2023-02-18',
                    '2023-02-19',
                    '2023-02-20',
                    '2023-02-21',
                    '2023-02-22'
                )
                and consorcio
                in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
            group by 1, 2, 3
        ),

        -- 3.1. Lista os serviços conforme tratamento indicado em resposta aos ofícios
        -- MTR-OFI-2024/03024, MTR-OFI-2024/03025, MTR-OFI-2024/03026 e
        -- MTR-OFI-2024/03027 (Processo MTR-PRO-2024/06270)
        rdo_correcao_servico as (
            select distinct
                data_inicio_quinzena,
                data_final_quinzena,
                servico_tratado_rdo,
                servico_corrigido_rioonibus
            from {{ ref("rdo_correcao_rioonibus_servico_quinzena") }}
        ),

        -- 3.2. Adiciona serviços corrigidos do RDO
        rdo_servico_corrigido as (
            select
                data,
                consorcio,
                rdo.servico,
                cro.servico_corrigido_rioonibus,
                receita_tarifaria_aferida
            from rdo
            left join
                rdo_correcao_servico as cro
                on rdo.data between cro.data_inicio_quinzena and cro.data_final_quinzena
                and rdo.servico = cro.servico_tratado_rdo
        ),

        -- 3.3. Corrige serviços do RDO com base nos serviços subsidiados (ou seja,
        -- apenas serviços planejados no dia)
        rdo_corrigido as (
            select
                rdo.data,
                ksf.consorcio,
                ksf.servico,
                sum(receita_tarifaria_aferida) as receita_tarifaria_aferida
            from rdo_servico_corrigido as rdo
            left join
                km_subsidiada_filtrada as ksf
                on rdo.data = ksf.data
                and (
                    rdo.servico_corrigido_rioonibus = ksf.servico
                    or rdo.servico = ksf.servico
                )
            group by 1, 2, 3
        ),

        -- 4. Calcula valores esperados de receita e subsídio com base nos parâmetros
        -- de remuneração por km
        parametros as (
            select distinct
                data_inicio,
                data_fim,
                irk,
                case
                    when data_fim <= "2022-12-31"
                    then irk - subsidio_km  -- subsidio varia ao longo dos meses
                    else
                        coalesce(
                            irk_tarifa_publica,
                            irk - (subsidio_km + desconto_subsidio_km)
                        )
                end as irk_tarifa_publica,
                (subsidio_km + desconto_subsidio_km) as subsidio_km
            from {{ source("projeto_subsidio_sppo_encontro_contas", "parametros_km") }}
            where data_inicio >= "2022-06-01" and data_fim <= "2023-12-31"
        )
    select
        *,
        ifnull(receita_total_aferida, 0)
        - ifnull(receita_total_esperada - subsidio_glosado, 0) as saldo
    from
        (
            select
                ks.* except (subsidio_pago),
                ks.km_subsidiada * par.irk as receita_total_esperada,
                ks.km_subsidiada * par.irk_tarifa_publica as receita_tarifaria_esperada,
                ks.km_subsidiada * par.subsidio_km as subsidio_esperado,
                case
                    when data >= "2023-01-01"
                    then (ks.km_subsidiada * par.subsidio_km - subsidio_pago)
                    else 0
                end as subsidio_glosado,
                ifnull(rdo.receita_tarifaria_aferida, 0)
                + ifnull(ks.subsidio_pago, 0) as receita_total_aferida,
                rdo.receita_tarifaria_aferida,
                ks.subsidio_pago
            from km_subsidiada_filtrada ks
            left join rdo_corrigido as rdo using (data, servico, consorcio)
            left join parametros par on ks.data between data_inicio and data_fim
        )
{% else %} {{ config(enabled=false) }}
{% endif %}
