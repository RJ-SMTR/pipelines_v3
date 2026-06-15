with
    -- 1. Lista pares data-serviço atípicos (pares de dia e serviço que registraram
    -- ocorrências com potencial de impactar a operação e comprometer a apuração das
    -- viagens válidas. Nesses casos, não é possível estimar a receita esperada com
    -- base na quilometragem percorrida e, mesmo após recurso, ainda não incorporados
    -- ao datalake house)
    servico_dia_atipico as (
        select distinct data, servico from {{ ref("servico_dia_atipico") }}
    ),
    -- 2. Lista pares data-serviço corrigidos do RDO
    receita_tarifa_publica as (
        select data, servico, receita_tarifaria_aferida
        from {{ ref("aux_receita_tarifa_publica_dia") }}
    ),
    -- 3. Adiciona indicador e faz tratamento em pares data-serviço corrigidos com POD
    -- >= 80%
    subsidio_dia_pod_80 as (
        select
            data,
            consorcio,
            servico,
            if(perc_km_planejada >= 80, km_apurada, null) as km_apurada,
            if(
                perc_km_planejada >= 80, valor_subsidio_pago, null
            ) as valor_subsidio_pago,
            perc_km_planejada < 80 as indicador_pod_menor_80
        from {{ ref("aux_subsidio_servico_dia") }}
    ),
    -- 4. Agrega pares data-serviço do subsídio (em aux_subsidio_servico_dia há
    -- correção dos serviços e pares podem ter o mesmo serviço, sendo necessário
    -- agregá-los nesta etapa)
    subsidio_dia_corrigido_agg as (
        select
            data,
            max_by(consorcio, km_apurada) as consorcio,
            servico,
            sum(km_apurada) as km_apurada,
            sum(valor_subsidio_pago) as valor_subsidio_pago,
            count(*) as datas_servico,
            sum(
                case when indicador_pod_menor_80 then 0 else 1 end
            ) as datas_servico_pod_maior_80,
            sum(
                case when indicador_pod_menor_80 then 1 else 0 end
            ) as datas_servico_pod_menor_80,
        from subsidio_dia_pod_80
        group by all
    ),
    -- 5. Associa pares data-serviço subsidiados aos pares data-serviço com receita
    -- tarifária aferida
    subsidio_dia_rdo as (
        select
            sd.*,
            -- Se a km_apurada é nula, POD < 80%
            if(
                receita_tarifaria_aferida is not null and sd.km_apurada is null,
                null,
                receita_tarifaria_aferida
            ) as receita_tarifaria_aferida,
            data in (
                {{ var("encontro_contas_datas_excecoes").keys() | join(", ") }}
            ) as indicador_data_excecao  -- Datas de exceção que serão desconsideradas no encontro de contas
        from subsidio_dia_corrigido_agg as sd
        left join receita_tarifa_publica using (data, servico)
    )
-- 6. Inclui indicador de pares data-serviço atípicos
select
    s.*,
    sda.data is not null as indicador_atipico,
    -- Corrigir em razão do tratamento anterior
    receita_tarifaria_aferida is null as indicador_ausencia_receita_tarifaria
from subsidio_dia_rdo as s
left join servico_dia_atipico as sda using (data, servico)
