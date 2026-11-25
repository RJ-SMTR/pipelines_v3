{% macro replay_subsidy() %} {{ return(var("replay_subsidy", none)) }} {% endmacro %}

{% macro subsidy_replay_dataset(replay) %}
    {% if target.name != "dev" or target.database != "rj-smtr-dev" %}
        {{
            exceptions.raise_compiler_error(
                "Subsidy replay requires the dev target in rj-smtr-dev."
            )
        }}
    {% endif %}

    {% if replay is not mapping or replay.service is not string or not replay.service | trim or replay.feed_start_date is not string or not modules.re.fullmatch(
        "[0-9]{4}-[0-9]{2}-[0-9]{2}", replay.feed_start_date
    ) or replay.dataset_prefix is not string or not replay.dataset_prefix %}
        {{
            exceptions.raise_compiler_error(
                "Set replay_subsidy with a quoted service, a quoted YYYY-MM-DD feed_start_date, and a dataset_prefix."
            )
        }}
    {% endif %}

    {% set suffix = replay.get("dataset_suffix", "") %}
    {% set dataset = replay.dataset_prefix ~ "_reprocessamento" %}
    {% if suffix %} {% set dataset = dataset ~ "_" ~ suffix %} {% endif %}
    {% if not modules.re.fullmatch("[A-Za-z0-9_]{1,1024}", dataset) %}
        {{
            exceptions.raise_compiler_error(
                "The replay dataset name must have at most 1,024 letters, digits, or underscores."
            )
        }}
    {% endif %}
    {{ return(dataset) }}
{% endmacro %}
