{{ config(materialized="ephemeral") }}

{% if "23:59:59" in var("date_range_end") %}
    {% set partition_filter %}
        ({{ generate_date_hour_partition_filter(var('date_range_start'), add_to_datetime(var("date_range_end"), seconds=1)) }})
    {% endset %}
{% else %}
    {% set partition_filter %}
        ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    {% endset %}
{% endif %}

-- 1. Filtra realocações válidas dentro do intervalo de GPS avaliado
with
    realocacao as (
        select
            * except (datetime_saida),
            case
                when datetime_saida is null then datetime_operacao else datetime_saida
            end as datetime_saida
        from {{ ref("staging_realocacao") }}
        where
            data between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
            -- Realocação deve acontecer após o registro de GPS e até 1 hora depois
            and datetime_diff(datetime_operacao, datetime_entrada, minute)
            between 0 and 60
            and (
                datetime_saida >= datetime("{{var('date_range_start')}}")
                or datetime_operacao >= datetime("{{var('date_range_start')}}")
            )
    ),
    -- 2. Altera registros de GPS com servicos realocados
    gps as (
        select *
        from {{ ref("staging_gps") }}
        where
            {{ partition_filter }}
            and datetime_gps
            between datetime("{{ var('date_range_start') }}") and datetime(
                "{{ var('date_range_end') }}"
            )
    ),
    servicos_realocados as (
        select
            g.* except (servico),
            g.servico as servico_gps,
            r.servico as servico_realocado,
            r.datetime_operacao as datetime_realocado,
            r.datetime_captura as datetime_captura_realocacao,
        from gps g
        inner join
            realocacao r
            on g.id_veiculo = r.id_veiculo
            and g.datetime_gps between r.datetime_entrada and r.datetime_saida
    ),
    gps_com_realocacao as (
        select
            g.* except (servico),
            coalesce(s.servico_realocado, g.servico) as servico,
            s.datetime_realocado,
            s.datetime_captura_realocacao
        from gps g
        left join servicos_realocados s using (id_veiculo, datetime_gps)
    )
-- Filtra realocacao mais recente para cada timestamp
select * except (rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_veiculo, datetime_gps
                order by datetime_captura_realocacao desc, datetime_realocado desc
            ) as rn
        from gps_com_realocacao
    )
where rn = 1
