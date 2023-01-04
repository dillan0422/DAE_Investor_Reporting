/** Macro to avoid custom schemas to be concatenated with default schema **/
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

        {{ custom_schema_name }}

{%- endmacro %}