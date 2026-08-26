{{
    config(
        alias="view_ativacao_dia",
    )
}}

with
    ativacao as (
        select data, id_area, sum(quantidade_ativacao) as quantidade_ativacao
        from {{ ref("ativacao_hora_riorotativo") }}
        group by all
    ),
    datas as (
        select data,
        from
            unnest(
                generate_date_array(
                    date("2026-07-12"),
                    current_date("America/Sao_Paulo"),
                    interval 1 day
                )
            ) as data
    ),
    area_estacionamento as (
        select
            a.id_area,
            a.nome as nome_area,
            a.logradouro as logradouro_area,
            st_centroid(a.geometry) as centroide,
            a.quantidade_vaga_total
            - ifnull(a.quantidade_vaga_idoso, 0)
            - ifnull(a.quantidade_vaga_pcd, 0)
            - safe_cast(
                floor(ifnull(quantidade_vaga_moto, 0) / 5) as int64
            ) as quantidade_vaga_fisica,
            a.data_inicio_vigencia,
            a.data_fim_vigencia,
            a.id_perfil_funcionamento
        from {{ ref("area_estacionamento_riorotativo") }} a
    ),
    area_estacionamento_data as (
        select d.*, a.* except (centroide) from area_estacionamento a cross join datas d
    ),
    area_estacionamento_data_perfil as (
        select
            a.*,
            array_concat_agg(
                array(
                    select extract(hour from t)
                    from
                        unnest(
                            generate_timestamp_array(
                                timestamp(datetime(data, p.horario_inicio)),
                                timestamp(datetime(data, p.horario_fim)),
                                interval 1 hour
                            )
                        ) t
                )
            ) horas
        from area_estacionamento_data a
        left join
            {{ ref("perfil_funcionamento_riorotativo") }} p
            on p.id_perfil_funcionamento in unnest(a.id_perfil_funcionamento)
            and extract(dayofweek from a.data) in unnest(p.dias_semana)
        group by all
    ),
    area_estacionamento_data_perfil_tratado as (
        select
            adp.* except (horas),
            a.centroide,
            array_length(
                array(select distinct h from unnest(adp.horas) h)
            ) as quantidade_horas,
        from area_estacionamento_data_perfil adp
        join area_estacionamento a using (id_area)
    ),
    area_data_ativacao as (
        select
            data,
            id_area,
            ae.nome_area,
            ae.logradouro_area,
            ae.centroide,
            ae.quantidade_vaga_fisica,
            floor(ae.quantidade_horas / 2)
            * ae.quantidade_vaga_fisica as capacidade_teorica,
            ifnull(atv.quantidade_ativacao, 0) as quantidade_ativacao
        from area_estacionamento_data_perfil_tratado ae
        left join ativacao atv using (data, id_area)
    )
select *
from area_data_ativacao
