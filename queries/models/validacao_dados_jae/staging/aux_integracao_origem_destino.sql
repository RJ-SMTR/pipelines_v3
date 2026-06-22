{{
    config(
        materialized="table",
    )
}}

{% set integracao_table = ref("integracao") %}

{% set var_is_incremental = not flags.FULL_REFRESH and table_exists(this) %}

{% if execute and var_is_incremental %}
    {% set partitions_query %}
        select concat("'", parse_date("%Y%m%d", partition_id), "'") as data
        from
            `{{ integracao_table.database }}.{{ integracao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = "{{ integracao_table.identifier }}"
            and partition_id != "__NULL__"
            and date(last_modified_time, "America/Sao_Paulo")
            between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% set adjacent_partitions_query %}
        with base as (
            select
                parse_date("%Y%m%d", partition_id) as data
            from
                `{{ integracao_table.database }}.{{ integracao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ integracao_table.identifier }}"
                and partition_id != "__NULL__"
                and date(last_modified_time, "America/Sao_Paulo")
                    between date("{{ var('date_range_start') }}")
                    and date("{{ var('date_range_end') }}")
        )
        select concat("'", data, "'") as data
        from (
            select date_sub(data, interval 1 day) as data from base
            union distinct
            select date_add(data, interval 1 day) from base
        )
    {% endset %}

    {% set adjacent_partitions = (
        run_query(adjacent_partitions_query).columns[0].values()
    ) %}

    {% if partitions | length > 0 %}
        {% set final_partitions_query %}
            select distinct concat("'", date(datetime_processamento_integracao), "'")
            from {{ integracao_table }}
            where
                data in ({{ partitions | join(", ") }})
                or data in ({{ adjacent_partitions | join(", ") }})
        {% endset %}

        {% set final_partitions = (
            run_query(final_partitions_query).columns[0].values()
        ) %}

    {% else %} {% set final_partitions = [] %}
    {% endif %}
{% else %} {% set partitions = [] %}
{% endif %}


with
    integracao_particao_modificada as (
        select date(cast(p as string)) as particao
        from unnest([{{ partitions | join(", ") }}]) as p
    ),
    integracao as (
        /*
        1. Altera a informação de modo para do padrão da Jaé para o padrão da matriz
        2. Filtra partições modificadas
        */
        select
            i.*,
            ifnull(
                sm.modos,
                case
                    when i.modo = 'Van'
                    then [consorcio]
                    when
                        i.modo = 'Ônibus'
                        and not (
                            length(ifnull(regexp_extract(i.servico_jae, r'[0-9]+'), ''))
                            = 4
                            and ifnull(regexp_extract(i.servico_jae, r'[0-9]+'), '')
                            like '2%'
                        )
                    then ['SPPO']
                    when
                        i.modo = 'BRT'
                        and ifnull(l.tarifa_ida, l.tarifa_volta) > tp.valor_tarifa
                    then ['BRT ESP']
                    else [i.modo]
                end
            ) as modos
        from {{ integracao_table }} i
        left join integracao_particao_modificada pm on i.data = pm.particao
        left join {{ ref("aux_matriz_servico_modo") }} sm using (id_servico_jae)
        left join
            {{ ref("aux_linha_tarifa") }} l
            on i.id_servico_jae = l.cd_linha
            and i.datetime_transacao >= l.dt_inicio_validade
            and (
                l.data_fim_validade is null
                or i.datetime_transacao < l.data_fim_validade
            )
        where
        {% if var_is_incremental %}
                {% if partitions | length > 0 %}
                    data in ({{ partitions | join(", ") }})
                    or data in ({{ adjacent_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
            qualify max(pm.particao is not null) over (partition by id_integracao)
            {% else %} data >= "2000-01-01"
            {% endif %}
    ),
    integracao_origem_destino as (
        /*
        Altera o formato da tabela de integração adicionando informações da transação de destino
        */
        select
            date(datetime_processamento_integracao) as data,
            data as data_transacao,
            lead(data) over (win) as data_lead,
            id_integracao,
            sequencia_integracao,
            modos as modos_origem,
            id_servico_jae as id_servico_jae_origem,
            servico_jae as servico_jae_origem,
            lead(modos) over (win) as modos_destino,
            lead(id_servico_jae) over (win) as id_servico_jae_destino,
            lead(servico_jae) over (win) as servico_jae_destino,
            datetime_transacao as datetime_transacao_origem,
            lead(datetime_transacao) over (win) as datetime_transacao_destino,
            round(
                ifnull(cast(percentual_rateio as numeric), 0), 2
            ) as percentual_rateio_origem,
            round(
                ifnull(lead(cast(percentual_rateio as numeric)) over (win), 0), 2
            ) as percentual_rateio_destino
        from integracao
        window win as (partition by id_integracao order by sequencia_integracao)
    )
select
    *,
    case
        when
            row_number() over (win) > 1
            array_to_string(modos_origem, "|") as modos_origem_string,
            array_to_string(modos_destino, "|") as modos_destino_string,
        then
            concat(
                string_agg(
                    concat("(?:", array_to_string(modos_origem, "|"), ")"),
                    '-'
                ) over (
                    partition by id_integracao
                    order by sequencia_integracao
                    rows between unbounded preceding and current row
                ),
                "$"
            )
    end as integracao_origem_regex,
    row_number() over (win) as rn
from integracao_origem_destino
where array_length(modos_destino) > 0
window win as (partition by id_integracao order by sequencia_integracao)
