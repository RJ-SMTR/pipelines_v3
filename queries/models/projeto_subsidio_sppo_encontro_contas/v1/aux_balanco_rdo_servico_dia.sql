{% if var("start_date") >= var("DATA_SUBSIDIO_V9_INICIO") %} {{ config(enabled=false) }}
{% else %}
    {{
        config(
            materialized="ephemeral",
        )
    }}
{% endif %}

-- 0. Lista servicos e dias atípicos (pagos por recurso)
with
    recursos as (
        select data, id_recurso, tipo_recurso, servico, sum(valor_pago) as valor_pago
        from {{ ref("recursos_sppo_servico_dia_pago") }}
        -- `rj-smtr`.`br_rj_riodejaneiro_recursos`.`recursos_sppo_servico_dia_pago`
        group by 1, 2, 3, 4
    ),
    servico_dia_atipico as (
        select distinct data, servico
        from recursos
        where
            -- Quando o valor do recurso pago for R$ 0, desconsidera-se o recurso, pois:
            -- Recurso pode ter sido cancelado (pago e depois revertido)
            -- Problema reporto não gerou impacto na operação (quando aparece apenas 1
            -- vez)
            valor_pago != 0
            -- Desconsideram-se recursos do tipo "Algoritmo" (igual a apuração em
            -- produção, levantado pela TR/SUBTT/CMO)
            -- Desconsideram-se recursos do tipo "Viagem Individual" (não afeta
            -- serviço-dia)
            and tipo_recurso not in ("Algoritmo", "Viagem Individual")
            -- Desconsideram-se recursos de reprocessamento que já constam em produção
            and not (
                data between "2022-06-01" and "2022-06-30"
                and tipo_recurso = "Reprocessamento"
            )
    ),

    -- 3. Calcula a receita tarifaria por servico e dia
    rdo as (
        select
            data,
            consorcio,
            case
                when length(linha) < 3
                then lpad(linha, 3, "0")
                else
                    concat(
                        ifnull(regexp_extract(linha, r"[B-Z]+"), ""),
                        ifnull(regexp_extract(linha, r"[0-9]+"), "")
                    )
            end as servico,
            linha,
            tipo_servico,
            ordem_servico,
            round(
                sum(receita_buc)
                + sum(receita_buc_supervia)
                + sum(receita_cartoes_perna_unica_e_demais)
                + sum(receita_especie),
                0
            ) as receita_tarifaria_aferida
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
            and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
            and (length(linha) != 4 and linha not like "2%")  -- Remove rodoviarios
        group by 1, 2, 3, 4, 5, 6
    ),
    -- Remove servicos nao subsidiados
    sumario_dia as (
        select
            data,
            consorcio,
            servico,
            sum(km_apurada) as km_subsidiada,
            sum(valor_subsidio_pago) as subsidio_pago
        from {{ ref("sumario_servico_dia_historico") }}
        -- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
        where data between "2022-06-01" and "2023-12-31" and valor_subsidio_pago = 0
        group by 1, 2, 3
    ),
    rdo_filtrada as (
        select rdo.*
        from
            (
                select *
                from rdo
                left join servico_dia_atipico sda using (data, servico)
                where sda.data is null
            ) rdo
        left join sumario_dia sd using (data, servico)
        where sd.servico is null
    )
select
    bsd.data,
    bsd.consorcio,
    bsd.servico,
    bsd.km_subsidiada,
    bsd.receita_tarifaria_aferida,
    rdo.data as data_rdo,
    rdo.consorcio as consorcio_rdo,
    rdo.servico as servico_tratado_rdo,
    rdo.linha as linha_rdo,
    rdo.tipo_servico as tipo_servico_rdo,
    rdo.ordem_servico as ordem_servico_rdo,
    rdo.receita_tarifaria_aferida as receita_tarifaria_aferida_rdo
from {{ ref("balanco_servico_dia" ~ var("encontro_contas_modo"), v=1) }} bsd
full join rdo_filtrada rdo using (data, servico)
