-- Referências
{% set rdo40_registros = ref("rdo40_registros") %}
{# {% set rdo40_registros = "rj-smtr.br_rj_riodejaneiro_rdo.rdo40_registros" %} #}
with
    -- 1. Lista pares data-serviço com correção do serviço (serviço corrigido é o
    -- serviço correto que deve ser utilizado no encontro de contas)
    correcao_servico_rdo as (
        select *
        from {{ ref("correcao_servico_rdo") }}
        where tipo = "Sem planejamento porém com receita tarifária"
    ),
    -- 2. Calcula a receita tarifária para cada par data-serviço
    rdo_raw as (
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
            linha,
            tipo_servico,
            ordem_servico,
            safe_cast(
                (
                    sum(receita_buc)
                    + sum(receita_buc_supervia)
                    + sum(receita_cartoes_perna_unica_e_demais)
                    + sum(receita_especie)
                ) as numeric
            ) as receita_tarifaria_aferida
        from {{ rdo40_registros }}
        where
            data >= "{{ var('encontro_contas_datas_v2_inicio') }}"
            and data between "{{ var('start_date') }}" and "{{ var('end_date') }}"
            and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
            and not (
                length(ifnull(regexp_extract(linha, r"[0-9]+"), "")) = 4
                and ifnull(regexp_extract(linha, r"[0-9]+"), "") like "2%"
            )  -- Remove rodoviários
        group by all
    ),
    -- 3. Remove pares data-serviço sem receita tarifária aferida
    rdo_filtrado as (select * from rdo_raw where receita_tarifaria_aferida != 0)
-- 4. Associa serviço corrigido aos pares data-serviço do RDO
select
    r.* except (servico),
    servico as servico_original_rdo,
    coalesce(servico_corrigido, servico) as servico
from rdo_filtrado as r
left join correcao_servico_rdo using (data, servico)
