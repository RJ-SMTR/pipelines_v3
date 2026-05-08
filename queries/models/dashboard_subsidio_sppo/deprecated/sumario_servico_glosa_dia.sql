with
    -- 1. Recupera valor (subsidio pago e penalidade), km apurada e POD do dia
    sumario_dia as (
        select
            data,
            consorcio,
            servico,
            perc_km_planejada,
            km_apurada,
            round(valor_subsidio_pago, 2) as valor_subsidio_pago,
            valor_penalidade
        from {{ ref("sumario_servico_dia") }}
        where data between "2023-01-16" and "2023-08-31"
    ),
    -- 2. Recupera km apurada por tipo de viagem
    sumario_tipo_viagem as (
        select data, consorcio, servico, tipo_viagem, km_apurada
        from {{ ref("sumario_servico_tipo_viagem_dia") }}
        where
            data between "2023-01-16" and "2023-08-31"
            and tipo_viagem != "Não licenciado"
    ),
    -- 2. Calcula valor glosado do subsidio por tipo de viagem
    parametros_tipo_viagem as (
        select
            data_inicio,
            data_fim,
            case
                -- WHEN status = "Nao licenciado" THEN "Não licenciado"
                when status = "Licenciado com ar e autuado (023.II)"
                then "Autuado por ar inoperante"
                when status = "Licenciado sem ar"
                then "Licenciado sem ar e não autuado"
                when status = "Licenciado com ar e não autuado (023.II)"
                then "Licenciado com ar e não autuado"
                else status
            end as tipo_viagem,
            subsidio_km,
        from {{ ref("subsidio_parametros") }}
        where data_fim >= "2023-01-16" and data_inicio <= "2023-08-31"
    ),
    subsidio_tipo_viagem as (
        select
            data,
            consorcio,
            servico,
            round(sum(t.km_apurada), 2) as km_apurada_tipo_viagem,
            ifnull(
                round(sum(t.km_apurada * p.subsidio_km), 2), 0
            ) as valor_subsidio_tipo_viagem,
            ifnull(
                round(sum(t.km_apurada * (2.81 - p.subsidio_km)), 2), 0
            ) as glosa_subsidio_tipo_viagem
        from sumario_tipo_viagem t
        left join
            parametros_tipo_viagem p
            on t.data between p.data_inicio and p.data_fim
            and t.tipo_viagem = p.tipo_viagem
        group by 1, 2, 3
    )
select
    data,
    consorcio,
    servico,
    perc_km_planejada,
    km_apurada,  -- Considera veículos não licenciados (continuam sem subsídio)
    km_apurada_tipo_viagem,  -- Desconsidera veículos não licenciados
    (valor_subsidio_pago + valor_penalidade) as valor_subsidio_com_glosa,
    valor_subsidio_tipo_viagem,
    -- Quando há subsidio, a glosa é o pagamento diferenciado por km dos tipos de viagem
    case
        when valor_subsidio_pago > 0 then glosa_subsidio_tipo_viagem else 0
    end as glosa_subsidio_tipo_viagem,
    -- Quando não subsidio, a glosa é o não pagamento do subsidio + penalidade (< 40%
    -- ou <60%)
    case
        when valor_subsidio_pago = 0 then round(- valor_penalidade, 2) else 0
    end as glosa_penalidade
from sumario_dia s
inner join subsidio_tipo_viagem t using (data, consorcio, servico)
