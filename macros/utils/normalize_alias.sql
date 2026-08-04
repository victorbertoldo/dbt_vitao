{% macro normalize_alias(name) %}
  {%- set result = name
    | lower
    | replace(' ', '_')
    | replace('.', '_')
    | replace(':', '_')
    | replace('@', '')
    | replace('-', '_')
    | replace('/', '_')
    | replace('"', '')
    | replace("'", '')
    | replace('(', '')
    | replace(')', '')
    | replace('á', 'a') | replace('à', 'a') | replace('â', 'a') | replace('ã', 'a') | replace('ä', 'a')
    | replace('é', 'e') | replace('è', 'e') | replace('ê', 'e') | replace('ë', 'e')
    | replace('í', 'i') | replace('ì', 'i') | replace('î', 'i') | replace('ï', 'i')
    | replace('ó', 'o') | replace('ò', 'o') | replace('ô', 'o') | replace('õ', 'o') | replace('ö', 'o')
    | replace('ú', 'u') | replace('ù', 'u') | replace('û', 'u') | replace('ü', 'u')
    | replace('ç', 'c')
    | replace('ñ', 'n')
  -%}
  {{ return(result) }}
{% endmacro %}
