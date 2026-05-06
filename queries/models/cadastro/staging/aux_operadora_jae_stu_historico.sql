{{
    config(
        materialized="table",
    )
}}

with
    operadora_jae_stu as (
        select
            ifnull(
                greatest(j.datetime_inicio_validade, s.datetime_inicio_validade),
                j.datetime_inicio_validade
            ) as datetime_inicio_validade,
            j.id_operadora_jae,
            s.perm_autor as id_operadora_stu,
            j.id_cliente,
            j.modo as modo_jae,
            s.modo as modo_stu,
            j.tipo_documento,
            j.documento,
            j.operadora as operadora_jae,
            s.nome_operadora as operadora_stu,
            j.indicador_operador_ativo_jae
        from {{ ref("aux_operadora_cliente_join_jae_historico") }} j
        left join
            {{ ref("aux_operadora_stu") }} s
            on j.documento = s.documento
            and j.tipo_documento = s.tipo_documento
            and s.modo = j.modo_join
            and (
                (
                    s.datetime_inicio_validade >= j.datetime_inicio_validade
                    and (
                        s.datetime_inicio_validade < j.datetime_fim_validade
                        or j.datetime_fim_validade is null
                    )
                )
                or (
                    s.datetime_fim_validade is not null
                    and s.datetime_fim_validade >= j.datetime_inicio_validade
                    and (
                        s.datetime_fim_validade < j.datetime_fim_validade
                        or j.datetime_fim_validade is null
                    )
                )
                or (
                    s.datetime_inicio_validade < j.datetime_inicio_validade
                    and (
                        s.datetime_fim_validade >= j.datetime_fim_validade
                        or s.datetime_fim_validade is null
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
        from operadora_jae_stu
    )
select *
from fim_validade
