select data, servico, sum(receita_tarifa_publica_faixa) as receita_tarifaria_aferida
from
    `rj-smtr-dev.victor__financeiro_interno.diferenca_tarifaria_sumario_servico_faixa_sentido`
where data between "{{ var('start_date') }}" and "{{ var('end_date') }}"
group by all
