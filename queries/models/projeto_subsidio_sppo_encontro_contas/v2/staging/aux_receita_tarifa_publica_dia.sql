with
    aux_rdo_servico_dia as (
        select
            data, servico, sum(receita_tarifaria_aferida) as receita_tarifaria_aferida
        from {{ ref("aux_rdo_servico_dia") }}
        where data < date("{{ var('DATA_SUBSIDIO_V18A_INICIO') }}")
        group by all
    ),
    aux_jae_servico_dia as (
        select data, servico, receita_tarifaria_aferida
        from {{ ref("aux_jae_servico_dia") }}
        where data >= date("{{ var('DATA_SUBSIDIO_V18A_INICIO') }}")
    )
select *
from aux_rdo_servico_dia
union all
select *
from aux_jae_servico_dia
