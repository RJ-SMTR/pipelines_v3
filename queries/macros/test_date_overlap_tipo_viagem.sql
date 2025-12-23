{% test date_overlap_tipo_viagem(model) -%}
    with
        base as (select distinct status, data_inicio, data_fim from {{ model }}),
        overlaps as (
            select
                a.status,
                a.data_inicio as a_inicio,
                a.data_fim as a_fim,
                b.data_inicio as b_inicio,
                b.data_fim as b_fim
            from base a
            join
                base b
                on a.status = b.status
                and a.data_inicio < b.data_fim
                and b.data_inicio < a.data_fim
                and a.data_inicio != b.data_inicio
                and a.data_fim != b.data_fim
        )
    select *
    from overlaps
{%- endtest %}
