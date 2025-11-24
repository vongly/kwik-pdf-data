{% macro check_for_source(schema_name, table_name) %}
    {% set test_query %}
        select 1
        from {{ schema_name }}.{{ table_name }}
        limit 1
    {% endset %}

    {% set result = run_query(test_query) %}

    {% if result is none %}
        {{ log("Skipping model because " ~ schema_name ~ "." ~ table_name ~ " does not exist", info=True) }}
        {{ return("skip") }}
    {% else %}
        {{ return("ok") }}
    {% endif %}
{% endmacro %}