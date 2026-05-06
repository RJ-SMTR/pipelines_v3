{{
    config(
        materialized="table",
    )
}}

with
    operadora_cliente as (
        select
            greatest(
                o.datetime_inicio_validade, c.datetime_inicio_validade
            ) as datetime_inicio_validade,
            o.id_operadora_jae,
            o.id_cliente,
            id_modo,
            modo,
            modo_join,
            tipo_documento,
            documento,
            nome as operadora,
            o.indicador_operador_ativo_jae
        from {{ ref("aux_operadora_jae_historico") }} o
        left join
            {{ ref("aux_operadora_jae_cliente_historico") }} c
            on o.id_cliente = c.id_cliente
            and (
                (
                    c.datetime_inicio_validade >= o.datetime_inicio_validade
                    and (
                        c.datetime_inicio_validade < o.datetime_fim_validade
                        or o.datetime_fim_validade is null
                    )
                )
                or (
                    c.datetime_fim_validade is not null
                    and c.datetime_fim_validade >= o.datetime_inicio_validade
                    and (
                        c.datetime_fim_validade < o.datetime_fim_validade
                        or o.datetime_fim_validade is null
                    )
                )
                or (
                    c.datetime_inicio_validade < o.datetime_inicio_validade
                    and (
                        c.datetime_fim_validade >= o.datetime_fim_validade
                        or c.datetime_fim_validade is null
                    )
                )
            )
    ),
    fim_validade as (
        select
            datetime_inicio_validade,
            lead(datetime_inicio_validade) over (
                partition by id_operadora_jae order by datetime_inicio_validade
            ) as datetime_fim_validade,
            * except (datetime_inicio_validade)
        from operadora_cliente
    )
select *
from fim_validade
