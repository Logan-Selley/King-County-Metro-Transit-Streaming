{#
  Use the configured schema verbatim ("staging", "marts") instead of dbt's
  default "<target>_<custom>". The schemas already exist, created by
  docker/initdb/01-schema.sql, and are what the rest of the project names.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
