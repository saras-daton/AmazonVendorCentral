{% macro multi_unnesting_custom(variable, variable1) %}

    {% if target.type =='snowflake' %}
    , LATERAL FLATTEN( input => PARSE_JSON({{variable}}.VALUE:{{variable1}}))
    {% else %}
    left join unnest({{variable}}.{{variable1}})
    {% endif %}
    
{% endmacro %}