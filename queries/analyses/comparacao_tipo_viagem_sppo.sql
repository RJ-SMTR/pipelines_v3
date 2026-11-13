-- Checagem pedida no review do PR 812.
-- Compara o tipo_viagem fechado (viagem_transacao) com a tabela já
-- materializada em produção, num período fechado. A precedência tem de
-- ser a mesma: tipo do status, depois "Detectado com ar inoperante",
-- depois "Sem transação" / "Validador fechado" /
-- "Validador associado incorretamente".
-- Troque as datas antes de rodar: dbt compile --select comparacao_tipo_viagem_sppo

select
    n.data,
    count(*) as viagens,
    countif(n.tipo_viagem is distinct from p.tipo_viagem) as divergencias
from {{ ref("viagem_transacao") }} as n
inner join
    `rj-smtr.subsidio.viagem_transacao` as p using (data, id_viagem)
where n.data between date("2026-09-01") and date("2026-09-15")
group by n.data
having divergencias > 0
order by n.data
