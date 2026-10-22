{% macro is_incremental() %}
    {# Offline CI compilation must not introspect a real warehouse relation. #}
    {% if env_var('DBT_CI_OFFLINE_COMPILE', 'false') | as_bool %}
        {{ return(false) }}
    {% elif not execute %}
        {{ return(false) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ return(
            relation is not none
            and relation.type == 'table'
            and model.config.materialized == 'incremental'
            and not should_full_refresh()
        ) }}
    {% endif %}
{% endmacro %}
