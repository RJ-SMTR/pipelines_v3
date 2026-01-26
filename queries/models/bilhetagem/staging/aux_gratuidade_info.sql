{{
    config(
        materialized="table",
        unique_key="id_unico",
        partition_by={
            "field": "id_cliente",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000, "interval": 10000},
        },
    )
}}

with
    gratuidade as (
        select
            * except (datetime_inicio_validade, datetime_fim_validade),
            datetime_inicio_validade as datetime_inicio_validade_gratuidade,
            datetime_fim_validade as datetime_fim_validade_gratuidade
        from {{ ref("aux_gratuidade") }}
    ),
    estudante as (
        select
            * except (datetime_inicio_validade, datetime_fim_validade),
            datetime_inicio_validade as datetime_inicio_validade_estudante,
            datetime_fim_validade as datetime_fim_validade_estudante
        from {{ ref("aux_estudante") }}
    ),
    menor_cadastro_estudante as (
        select
            id_cliente,
            min(datetime_inicio_validade_estudante) as menor_cadastro_estudante
        from estudante
        group by 1
    ),
    laudo_pcd as (
        select
            * except (datetime_inicio_validade, datetime_fim_validade),
            datetime_inicio_validade as datetime_inicio_validade_saude,
            datetime_fim_validade as datetime_fim_validade_saude
        from {{ ref("aux_laudo_pcd") }}
    ),
    menor_cadastro_laudo_pcd as (
        select
            id_cliente, min(datetime_inicio_validade_saude) as menor_cadastro_laudo_pcd
        from laudo_pcd
        group by 1
    ),
    gratuidade_estudante_com_cadastro as (
        select
            concat(g.id_cliente_gratuidade, "_", e.id_unico) as id_unico,
            g.id_cliente,
            g.id_gratuidade,
            g.id_cliente_gratuidade,
            g.tipo_gratuidade,
            e.numero_matricula,
            e.codigo_escola,
            e.nome_escola,
            e.rede_ensino,
            e.id_cre_escola,
            case
                when
                    g.datetime_inicio_validade_gratuidade
                    > e.datetime_inicio_validade_estudante
                then g.datetime_inicio_validade_gratuidade
                else e.datetime_inicio_validade_estudante
            end as datetime_inicio_validade_gratuidade,
            datetime_fim_validade_gratuidade
        from gratuidade g
        join estudante e using (id_cliente)
        where g.tipo_gratuidade = 'Estudante'
    ),
    gratuidade_estudante_sem_cadastro as (
        select
            g.id_cliente_gratuidade as id_unico,
            g.id_cliente,
            g.id_gratuidade,
            g.id_cliente_gratuidade,
            g.tipo_gratuidade,
            cast(null as string) as numero_matricula,
            cast(null as string) as codigo_escola,
            cast(null as string) as nome_escola,
            cast(null as string) as rede_ensino,
            cast(null as string) as id_cre_escola,
            g.datetime_inicio_validade_gratuidade,
            datetime_fim_validade_gratuidade
        from gratuidade g
        left join menor_cadastro_estudante m using (id_cliente)
        where
            g.tipo_gratuidade = 'Estudante'
            and (
                g.datetime_inicio_validade_gratuidade < m.menor_cadastro_estudante
                or m.menor_cadastro_estudante is null
            )
    ),
    gratuidade_estudante_completo as (
        select *
        from gratuidade_estudante_com_cadastro

        union all

        select *
        from gratuidade_estudante_sem_cadastro
    ),
    gratuidade_estudante_fim_validade as (
        select
            * except (datetime_fim_validade_gratuidade),
            lead(datetime_inicio_validade_gratuidade) over (
                partition by id_cliente order by datetime_inicio_validade_gratuidade
            ) as datetime_fim_validade_gratuidade
        from gratuidade_estudante_completo
        where
            datetime_inicio_validade_gratuidade < datetime_fim_validade_gratuidade
            or datetime_fim_validade_gratuidade is null
    ),
    gratuidade_saude_com_cadastro as (
        select
            concat(g.id_cliente_gratuidade, "_", l.id_laudo_pcd) as id_unico,
            g.id_cliente,
            g.id_gratuidade,
            g.id_cliente_gratuidade,
            g.tipo_gratuidade,
            l.deficiencia_permanente,
            case
                when
                    g.datetime_inicio_validade_gratuidade
                    > l.datetime_inicio_validade_saude
                then g.datetime_inicio_validade_gratuidade
                else l.datetime_inicio_validade_saude
            end as datetime_inicio_validade_gratuidade,
            datetime_fim_validade_gratuidade
        from gratuidade g
        join laudo_pcd l using (id_cliente)
        where g.tipo_gratuidade = 'PCD'
    ),
    gratuidade_saude_sem_cadastro as (
        select
            g.id_cliente_gratuidade as id_unico,
            g.id_cliente,
            g.id_gratuidade,
            g.id_cliente_gratuidade,
            g.tipo_gratuidade,
            cast(null as bool) as deficiencia_permanente,
            g.datetime_inicio_validade_gratuidade,
            datetime_fim_validade_gratuidade
        from gratuidade g
        left join menor_cadastro_laudo_pcd m using (id_cliente)
        where
            g.tipo_gratuidade = 'PCD'
            and (
                g.datetime_inicio_validade_gratuidade < m.menor_cadastro_laudo_pcd
                or m.menor_cadastro_laudo_pcd is null
            )
    ),
    gratuidade_saude_completo as (
        select *
        from gratuidade_saude_com_cadastro

        union all

        select *
        from gratuidade_saude_sem_cadastro
    ),
    gratuidade_saude_fim_validade as (
        select
            * except (datetime_fim_validade_gratuidade),
            lead(datetime_inicio_validade_gratuidade) over (
                partition by id_cliente order by datetime_inicio_validade_gratuidade
            ) as datetime_fim_validade_gratuidade
        from gratuidade_saude_completo
        where
            datetime_inicio_validade_gratuidade < datetime_fim_validade_gratuidade
            or datetime_fim_validade_gratuidade is null
    ),
    outras_gratuidades as (
        select
            id_cliente_gratuidade as id_unico,
            id_cliente,
            id_gratuidade,
            id_cliente_gratuidade,
            tipo_gratuidade,
            datetime_inicio_validade_gratuidade,
            datetime_fim_validade_gratuidade
        from gratuidade
        where tipo_gratuidade not in ('Estudante', 'PCD')
    ),
    union_gratuidade as (
        select
            id_unico,
            id_cliente,
            id_gratuidade,
            id_cliente_gratuidade,
            tipo_gratuidade,
            numero_matricula,
            codigo_escola,
            nome_escola,
            rede_ensino,
            id_cre_escola,
            cast(null as bool) as deficiencia_permanente,
            datetime_inicio_validade_gratuidade as datetime_inicio_validade,
            datetime_fim_validade_gratuidade as datetime_fim_validade,
            0 as priority
        from gratuidade_estudante_fim_validade

        union all

        select
            id_unico,
            id_cliente,
            id_gratuidade,
            id_cliente_gratuidade,
            tipo_gratuidade,
            cast(null as string) as numero_matricula,
            cast(null as string) as codigo_escola,
            cast(null as string) as nome_escola,
            cast(null as string) as rede_ensino,
            cast(null as string) as id_cre_escola,
            deficiencia_permanente,
            datetime_inicio_validade_gratuidade as datetime_inicio_validade,
            datetime_fim_validade_gratuidade as datetime_fim_validade,
            1 as priority
        from gratuidade_saude_fim_validade

        union all

        select
            id_unico,
            id_cliente,
            id_gratuidade,
            id_cliente_gratuidade,
            tipo_gratuidade,
            cast(null as string) as numero_matricula,
            cast(null as string) as codigo_escola,
            cast(null as string) as nome_escola,
            cast(null as string) as rede_ensino,
            cast(null as string) as id_cre_escola,
            cast(null as bool) as deficiencia_permanente,
            datetime_inicio_validade_gratuidade as datetime_inicio_validade,
            datetime_fim_validade_gratuidade as datetime_fim_validade,
            2 as priority
        from outras_gratuidades
    ),
    nova_validade as (
        select
            * except (datetime_fim_validade, priority),
            lead(datetime_inicio_validade) over (
                partition by id_cliente
                order by datetime_inicio_validade, priority, id_unico
            ) as datetime_fim_validade
        from union_gratuidade
    )
select *
from nova_validade
