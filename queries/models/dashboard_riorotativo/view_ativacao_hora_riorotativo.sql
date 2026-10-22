{{
    config(
        alias="view_ativacao_hora",
    )
}}

with
    datas as (
        select date(data_hora) as data, extract(hour from data_hora) as hora
        from
            unnest(
                generate_timestamp_array(
                    timestamp("2026-07-12 00:00:00"),
                    current_timestamp() - interval 3 hour,
                    interval 1 hour
                )
            ) as data_hora
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
        select
            d.*,
            a.*,
            d.data >= a.data_inicio_vigencia
            and (
                d.data <= a.data_fim_vigencia or a.data_fim_vigencia is null
            ) as indicador_vaga_vigente
        from area_estacionamento a
        cross join datas d
    ),
    area_estacionamento_data_perfil as (
        select a.*,
        from area_estacionamento_data a
        join
            {{ ref("perfil_funcionamento_riorotativo") }} p
            on p.id_perfil_funcionamento in unnest(a.id_perfil_funcionamento)
            and extract(dayofweek from a.data) in unnest(p.dias_semana)
            and time(a.hora, 0, 0)
            between time(p.horario_inicio) and time(p.horario_fim)
        qualify row_number() over (partition by data, hora, id_area) = 1
    ),
    data_area_ativacao as (
        select
            data,
            hora,
            id_area,
            ae.nome_area,
            ae.logradouro_area,
            ae.centroide,
            st_y(ae.centroide) as latitude_centroide,
            st_x(ae.centroide) as longitude_centroide,
            if(
                indicador_vaga_vigente, ae.quantidade_vaga_fisica, 0
            ) as quantidade_vaga_fisica,
            if(
                indicador_vaga_vigente, ae.quantidade_vaga_fisica, 0
            ) as capacidade_teorica,
            ifnull(atv.quantidade_ativacao, 0) as quantidade_ativacao
        from area_estacionamento_data_perfil ae
        left join {{ ref("ativacao_hora_riorotativo") }} atv using (data, hora, id_area)
        where indicador_vaga_vigente
    )
select *
from data_area_ativacao
