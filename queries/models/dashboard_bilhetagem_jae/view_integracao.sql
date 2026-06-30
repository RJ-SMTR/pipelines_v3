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
    ),
    dados_filtrados as (
        select
            i.data,
            i.hora,
            i.id_integracao,
            i.sequencia_integracao,
            i.modo,
            s.descricao_servico,
            i.consorcio,
            i.datetime_transacao
        from {{ ref("integracao") }} i
        left join servicos s using (id_servico_jae)
        where
            data >= "2024-02-24"
            and servico not in ("888888", "999999")
            and id_operadora != "2"
    )
select
    a.data,
    a.hora,
    a.id_integracao,
    a.sequencia_integracao as perna_origem,
    a.modo as modo_origem,
    concat(a.modo, '(', a.sequencia_integracao, ')') as modo_origem_perna,
    a.consorcio as consorcio_origem,
    a.descricao_servico as descricao_servico_origem,
    concat(
        a.descricao_servico, '(', a.sequencia_integracao, ')'
    ) as descricao_servico_origem_perna,
    b.sequencia_integracao as perna_destino,
    b.modo as modo_destino,
    concat(b.modo, '(', b.sequencia_integracao, ')') as modo_destino_perna,
    b.consorcio as consorcio_destino,
    b.descricao_servico as descricao_servico_destino,
    concat(
        b.descricao_servico, '(', b.sequencia_integracao, ')'
    ) as descricao_servico_destino_perna,
    timestamp_diff(
        b.datetime_transacao, a.datetime_transacao, minute
    ) as tempo_integracao_minutos
from dados_filtrados a
join
    dados_filtrados b
    on a.id_integracao = b.id_integracao
    and a.sequencia_integracao = b.sequencia_integracao - 1
