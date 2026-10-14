{% macro get_date_hour_partitions(relation) %}
    {% set partitions_query %}
        select distinct concat("'", date(data), "'") as data
        from {{ relation }}
        where
            {{
                generate_date_hour_partition_filter(
                    var("date_range_start"), var("date_range_end")
                )
            }}
    {% endset %}

    {{ return(run_query(partitions_query).columns[0].values()) }}
{% endmacro %}
