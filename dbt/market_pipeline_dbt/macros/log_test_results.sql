{% macro log_test_results(results) %}
  {% if execute %}
    {% for result in results %}
      {% if result.node.resource_type == 'test' %}
        {% set insert_sql %}
          INSERT INTO dbt_test_results (test_name, status)
          VALUES ('{{ result.node.name }}', '{{ result.status }}')
        {% endset %}
        {% do run_query(insert_sql) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
