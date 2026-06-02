{{ config(materialized="ephemeral") }}

with
    subsidio_dia as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            case
                when data < date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}') and rn = 1
                then min_pof
                when data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                then pof
                else null
            end as min_pof
        from
            (
                select
                    *,
                    min(pof) over (
                        partition by data, tipo_dia, consorcio, servico, sentido
                    ) as min_pof,
                    row_number() over (
                        partition by data, tipo_dia, consorcio, servico, sentido
                        order by pof, faixa_horaria_inicio
                    ) as rn
                from {{ ref("percentual_operacao_faixa_horaria") }}
                -- from `rj-smtr.subsidio.percentual_operacao_faixa_horaria`
                {% if is_incremental() %}
                    where
                        data between date('{{ var("start_date") }}') and date(
                            '{{ var("end_date") }}'
                        )
                {% endif %}
            )
    ),
    penalidade as (
        select
            data_inicio,
            data_fim,
            perc_km_inferior,
            perc_km_superior,
            ifnull(- valor, 0) as valor_penalidade
        from {{ ref("valor_tipo_penalidade") }}
    -- from `rj-smtr.dashboard_subsidio_sppo.valor_tipo_penalidade`
    ),
    subsidio_valor_penalidade as (
        select
            s.data,
            s.tipo_dia,
            s.consorcio,
            s.servico,
            s.sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            safe_cast(coalesce(pe.valor_penalidade, 0) as numeric) as valor_penalidade
        from subsidio_dia as s
        left join
            penalidade as pe
            on s.data between pe.data_inicio and pe.data_fim
            and s.min_pof >= pe.perc_km_inferior
            and s.min_pof < pe.perc_km_superior
    ),
    subsidio_valor_penalidade_ajustado as (
        select
            v.* except (valor_penalidade),
            ifnull(e.valor_penalidade, v.valor_penalidade) as valor_penalidade,
            e.data_inicio as data_inicio_excecao,
            e.faixa_horaria_inicio as faixa_horaria_inicio_excecao,
            e.faixa_horaria_fim as faixa_horaria_fim_excecao,
            e.servico as servico_excecao,
            e.priority
        from subsidio_valor_penalidade v
        left join
            {{ ref("aux_subsidio_penalidade_servico_faixa_excecao") }} e
            on v.data between e.data_inicio and e.data_fim
            and (
                v.faixa_horaria_inicio >= e.faixa_horaria_inicio
                or e.faixa_horaria_inicio is null
            )
            and (
                v.faixa_horaria_fim <= e.faixa_horaria_fim
                or e.faixa_horaria_fim is null
            )
            and (v.servico = e.servico or e.servico is null)
    ),
    subsidio_valor_penalidade_ajustado_deduplicado as (
        select
            * except (
                data_inicio_excecao,
                faixa_horaria_inicio_excecao,
                faixa_horaria_fim_excecao,
                servico_excecao,
                priority
            )
        from subsidio_valor_penalidade_ajustado
        qualify
            row_number() over (
                partition by
                    data,
                    consorcio,
                    servico,
                    sentido,
                    faixa_horaria_inicio,
                    faixa_horaria_fim
                order by priority nulls last
            )
            = 1
    )
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from subsidio_valor_penalidade_ajustado_deduplicado
