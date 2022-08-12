{%- macro select_all_except(
  source_schema,
  source_table,
  ignore_fields,
  skip_first_comma=False,
  alias=none
) -%}
{#
  return a list of columns separated by comma - all columns of a source table
  except the list provided.
#}
  {%- if not alias is none -%}
    {%- set alias = alias ~ '.' -%}
  {%- else -%}
    {%- set alias = '' -%}
  {%- endif -%}
  {%- set src = source(source_schema, source_table) -%}
  {%- call statement('columns', fetch_result=True) -%}
    SELECT column_name FROM information_schema.columns
    WHERE table_catalog = '{{ src.database }}'
      AND table_schema = '{{ src.schema }}'
      AND table_name = '{{ src.identifier }}'
      AND NOT column_name IN (
        ''{%- for field in ignore_fields -%},'{{field}}'{%- endfor -%}
      )
  {%- endcall -%}
  {%- set columns = load_result('columns')['data'] -%}
  {%- for column in columns -%}
    {%- if loop.first and skip_first_comma %}
    {{ alias }}{{ column[0] }}
    {%- else %}
    , {{ alias }}{{ column[0] }}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro -%}
