{#
  The relation's columns, minus some. Models select columns this way so a column added at the
  source (schema evolution) flows through to silver without editing SQL; on_schema_change then
  adds it to the incremental table.
#}
{% macro columns_of(relation, exclude=[], prefix='') %}
  {%- set names = [] -%}
  {%- for col in adapter.get_columns_in_relation(relation) if col.name not in exclude -%}
    {%- do names.append(prefix ~ adapter.quote(col.name)) -%}
  {%- endfor -%}
  {{ names | join(', ') }}
{% endmacro %}
