with
    tecnologia_servico as (
        select *
        from {{ ref("tecnologia_servico") }}
        where
            inicio_vigencia <= date('{{ var("date_range_end") }}')
            and (
                fim_vigencia is null
                or fim_vigencia >= date('{{ var("date_range_start") }}')
            )
    ),
    planejados as (
        select distinct
            data,
            tipo_dia,
            consorcio,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            distancia_total_planejada as km_planejada
        from {{ ref("viagem_planejada") }}
        where
            data between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
            and data < date('{{ var("DATA_SUBSIDIO_V25_INICIO") }}')
            and distancia_total_planejada > 0

        union all

        select distinct
            data,
            tipo_dia,
            consorcio,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            quilometragem as km_planejada
        from {{ ref("servico_planejado_faixa_horaria") }}
        where
            data between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
            and data >= date('{{ var("DATA_SUBSIDIO_V25_INICIO") }}')
            and quilometragem > 0
    ),
    servicos_validos as (select distinct servico from planejados),
    registros_invalidos as (
        select tecnologia_servico.*, 'maior_tecnologia_permitida' as column_name
        from tecnologia_servico
        where
            maior_tecnologia_permitida is null
            and servico in (select servico from servicos_validos)

        union all

        select tecnologia_servico.*, 'menor_tecnologia_permitida' as column_name
        from tecnologia_servico
        where
            menor_tecnologia_permitida is null
            and servico in (select servico from servicos_validos)
    )

select *
from registros_invalidos
