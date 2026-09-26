{% test variation_raisonnable(model, column_name, seuil_pct=30) %}

SELECT *
FROM {{ model }}
WHERE ABS({{ column_name }}) > {{ seuil_pct }}

{% endtest %}
