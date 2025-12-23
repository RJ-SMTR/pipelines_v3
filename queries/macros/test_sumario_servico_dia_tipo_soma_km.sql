{% test sumario_servico_dia_tipo_soma_km(model, column_name) %}
    -- depends_on: {{ ref('valor_km_tipo_viagem') }}
    {% if execute and "faixa" in model -%}
        {%- set results = generate_km_columns() -%}
        {%- set colunas_tipo_viagem = results.columns[1].values() -%}
        {%- set tipos_filtrados = (
            colunas_tipo_viagem
            | reject(
                "in",
                [
                    "licenciado_sem_ar_n_autuado",
                    "licenciado_com_ar_n_autuado",
                ],
            )
            | list
        ) -%}
    {%- endif -%}

    with
        kms as (
            select
                * except ({{ column_name }}),
                {{ column_name }},
                round(
                    {%- if "faixa" not in model -%}
                        coalesce(km_apurada_registrado_com_ar_inoperante, 0)
                        + coalesce(km_apurada_n_licenciado, 0)
                        + coalesce(km_apurada_autuado_ar_inoperante, 0)
                        + coalesce(km_apurada_autuado_seguranca, 0)
                        + coalesce(km_apurada_autuado_limpezaequipamento, 0)
                        + coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
                        + coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
                        + coalesce(km_apurada_n_vistoriado, 0)
                        + coalesce(km_apurada_sem_transacao, 0)
                    {%- else -%}
                        {%- for tipo in tipos_filtrados %}
                            coalesce(km_apurada_{{ tipo }}, 0)
                            {%- if not loop.last %} +{% endif %}
                        {%- endfor %}
                        + coalesce(km_apurada_total_licenciado_sem_ar_n_autuado, 0)
                        + coalesce(km_apurada_total_licenciado_com_ar_n_autuado, 0)
                    {%- endif -%},
                    2
                ) as km_apurada2
            from {{ model }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        )
    select *, abs(km_apurada2 -{{ column_name }}) as dif
    from kms
    where abs(km_apurada2 -{{ column_name }}) > 0.02
{%- endtest %}
