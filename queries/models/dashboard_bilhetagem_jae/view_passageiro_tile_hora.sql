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
    p.data,
    p.hora,
    p.modo,
    p.consorcio,
    p.id_servico_jae,
    s.servico,
    s.descricao_servico,
    concat(s.servico, ' - ', s.descricao_servico) as nome_completo_servico,
    p.sentido,
    p.cadastro_cliente,
    p.produto,
    p.tipo_transacao,
    ifnull(p.tipo_usuario, "Não Identificado") as tipo_usuario,
    p.meio_pagamento,
    p.tile_id,
    p.quantidade_passageiros
from {{ ref("passageiro_tile_hora") }} p
left join servicos s using (id_servico_jae)
