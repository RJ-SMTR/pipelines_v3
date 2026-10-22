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
    rdo_corrigido as (select * from {{ ref("aux_rdo_servico_dia") }}),
    -- 3. Lista pares data-serviço corrigidos do subsídio
    subsidio_dia_corrigido as (select * from {{ ref("aux_subsidio_servico_dia") }}),
    -- 4. Lista os pares data-serviço que estão sem planejamento porém com receita
    -- tarifária ou tem subsídio pago sem receita tarifária aferida
    subsidio_dia_rdo_corrigido as (
        select
            data,
            servico,
            coalesce(sd.consorcio, rdo.consorcio) as consorcio,
            case
                when rdo.servico is not null and sd.servico is null
                then "Sem planejamento porém com receita tarifária"
                when rdo.servico is null and sd.servico is not null
                then "Subsídio pago sem receita tarifária"
                else null
            end as tipo,
            linha,
            servico_original_rdo,
            servico_original_subsidio,
            tipo_servico,
            ordem_servico,
            receita_tarifaria_aferida,
            null as justificativa,
            null as servico_correto
        from subsidio_dia_corrigido as sd
        full join rdo_corrigido as rdo using (data, servico)
        where
            (
                (perc_km_planejada >= 80 and receita_tarifaria_aferida is null)
                or (receita_tarifaria_aferida is not null and perc_km_planejada is null)
            )
            and data
            not in ({{ var("encontro_contas_datas_excecoes").keys() | join(", ") }})  -- Remove datas de exceção que serão desconsideradas no encontro de contas
            and not (
                length(ifnull(regexp_extract(servico, r"[0-9]+"), "")) = 4
                and ifnull(regexp_extract(servico, r"[0-9]+"), "") like "2%"
            )  -- Remove rodoviários
    )
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from subsidio_dia_rdo_corrigido
