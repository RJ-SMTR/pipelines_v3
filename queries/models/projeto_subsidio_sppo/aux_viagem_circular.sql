-- 1. Identifica viagens circulares de ida que possuem volta
-- consecutiva. Junta numa única linha a datetime_partida (ida) + datetime_chegada_volta
with
    ida_volta_circular as (
        select t.*

        from {{ ref("aux_ida_volta_circular") }} t
        where
            flag_proximo_volta = true
            and sentido_shape = "I"
            and datetime_chegada <= datetime_partida_volta
    ),
    -- 2. Filtra apenas viagens circulares de ida e volta consecutivas
    -- (mantem ida e volta separadas, mas com o mesmo id)
    viagem_circular as (
        select distinct *
        from
            (
                select
                    case
                        when
                            (
                                v.sentido_shape = "I"
                                and v.datetime_partida = c.datetime_partida
                            )
                        then c.id_viagem
                        when
                            (
                                v.sentido_shape = "V"
                                and v.datetime_chegada = c.datetime_chegada_volta
                            )
                        then c.id_viagem
                    end as id_viagem,
                    v.* except (id_viagem)
                from {{ ref("aux_viagem_inicio_fim") }} v
                inner join
                    ida_volta_circular c
                    on c.id_veiculo = v.id_veiculo
                    and c.servico_realizado = v.servico_realizado
                    and c.sentido = v.sentido
                  {% if (
    (var("run_date") >= "2025-06-01" and var("run_date") <= "2025-06-30")
    or
    (var("run_date") >= var("DATA_SUBSIDIO_V24_INICIO"))
) %}
                        and c.shape_id_planejado = v.shape_id_planejado
                    {% endif %}
            ) v
        where id_viagem is not null
    )
-- 3. Junta viagens circulares tratadas às viagens não circulares já identificadas
select *
from viagem_circular v
union all
(
    select *
    from {{ ref("aux_viagem_inicio_fim") }} v
    where sentido = "I" or sentido = "V"
)
