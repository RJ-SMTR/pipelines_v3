{{ config(materialized="table", tags=["identificacao"]) }}

with
    operadora_jae_stu_cnpj as (
        select
        from {{ ref("aux_operadora_jae_stu_historico") }} js
        left join
            {{ ref("aux_operadora_cnpj") }} c
            on js.documento = c.documento
            and js.tipo_documento = 'CNPJ'
            and (
                (
                    c.datetime_inicio_validade >= js.datetime_inicio_validade
                    and (
                        c.datetime_inicio_validade < js.datetime_fim_validade
                        or js.datetime_fim_validade is null
                    )
                )
                or (
                    c.datetime_fim_validade is not null
                    and c.datetime_fim_validade >= js.datetime_inicio_validade
                    and (
                        c.datetime_fim_validade < js.datetime_fim_validade
                        or js.datetime_fim_validade is null
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
        from operadora_jae_stu_cnpj
    )
select *
from fim_validade
