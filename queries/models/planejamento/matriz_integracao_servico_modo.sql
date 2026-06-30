{{
    config(
        materialized="table",
    )
}}

with
    servicos as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_servico_jae order by data_inicio_vigencia
                    ) as rn
                from {{ ref("servicos") }}
            )
        where rn = 1
    )
select
    date(data_inicio) as data_inicio_validade,
    date(data_fim) as data_fim_validade,
    id_servico_jae,
    s.servico_jae,
    s.descricao_servico_jae,
    split(m.modos, ",") as modos
from {{ source("source_smtr", "matriz_servico_modo") }} m
join servicos s using (id_servico_jae)
