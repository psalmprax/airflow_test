{%- macro grant_read(schema, read_user) -%}
  GRANT USAGE ON SCHEMA {{ schema }} TO {{ read_user }};
  GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO {{ read_user }};
  ALTER DEFAULT PRIVILEGES IN SCHEMA {{ schema }}
  GRANT SELECT ON TABLES TO {{ read_user }};
{%- endmacro -%}

{%- macro grant_create(schema, create_user) -%}
  GRANT CREATE ON SCHEMA {{ schema }} TO {{ create_user }};
  GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO {{ create_user }};
  ALTER DEFAULT PRIVILEGES IN SCHEMA {{ schema }}
  GRANT SELECT ON TABLES TO {{ create_user }};
{%- endmacro -%}

