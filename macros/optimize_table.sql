{% macro optimize_table(model_name, partition_by=None, cluster_by=None) %}
    {% set table_name = model_name.split('.')[-1] %}
    {% set database = model_name.split('.')[0] %}
    {% set schema = model_name.split('.')[1] %}

    {% if partition_by %}
        ALTER TABLE {{ database }}.{{ schema }}.{{ table_name }}
        SET PARTITION BY ({{ partition_by }});
    {% endif %}

    {% if cluster_by %}
        ALTER TABLE {{ database }}.{{ schema }}.{{ table_name }}
        SET CLUSTER BY ({{ cluster_by }});
    {% endif %}
{% endmacro %} 