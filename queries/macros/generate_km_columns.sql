{%- macro generate_km_columns() -%}
    {%- set tipos_query -%}
        select distinct
            status as tipo_viagem,
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(
                                translate(
                                    status,
                                    'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                                    'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                                )
                            ),
                            r'[^\w\s]',  -- Remove caracteres não alfanuméricos
                            ''
                        ),
                        r'\b(e|por|de)\b',
                        ''  -- Remove as palavras 'e', 'por' e 'de'
                    ),
                    r'(^nao|\bnao\b)',
                    'n'  -- Substitui 'nao' por 'n'
                ),
                r'[\s]+',
                '_'  -- Substitui múltiplos espaços por um único "_"
            ) as coluna_tipo_viagem
        from {{ ref("valor_km_tipo_viagem") }}
        {# from `rj-smtr.subsidio.valor_km_tipo_viagem` #}
        where
            status not in ("Não classificado", "Nao licenciado", "Licenciado sem ar")
            and status not like '%(023.II)%'
        order by status
    {%- endset -%}

    {%- set results = run_query(tipos_query) -%}

    {{ return(results) }}
{%- endmacro -%}
