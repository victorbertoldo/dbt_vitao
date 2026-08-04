{#
  Normalizes an arbitrary source key (JSON key, column path, free-text label) into a
  valid unquoted Snowflake/SQL identifier. Source keys are not controlled by us -- they
  can be human-authored labels (survey questions, form field names) with unpredictable
  punctuation, so this transliterates known accented characters, then catches any other
  disallowed character generically rather than enumerating every symbol seen so far.
#}
{% macro normalize_alias(name) %}
  {%- set result = name
    | lower
    | replace('á', 'a') | replace('à', 'a') | replace('â', 'a') | replace('ã', 'a') | replace('ä', 'a')
    | replace('é', 'e') | replace('è', 'e') | replace('ê', 'e') | replace('ë', 'e')
    | replace('í', 'i') | replace('ì', 'i') | replace('î', 'i') | replace('ï', 'i')
    | replace('ó', 'o') | replace('ò', 'o') | replace('ô', 'o') | replace('õ', 'o') | replace('ö', 'o')
    | replace('ú', 'u') | replace('ù', 'u') | replace('û', 'u') | replace('ü', 'u')
    | replace('ç', 'c')
    | replace('ñ', 'n')
  -%}

  {#- Unquoted Snowflake identifiers only allow letters, digits, and underscore.
      Collapse any run of anything else (spaces, punctuation, emoji, etc.) into one underscore. #}
  {%- set result = modules.re.sub('[^a-z0-9_]+', '_', result) -%}
  {%- set result = modules.re.sub('_+', '_', result) -%}
  {%- set result = result.strip('_') -%}

  {#- Identifiers can't start with a digit (common with free-text keys like "19. Question text"). #}
  {%- if result != '' and modules.re.match('^[0-9]', result) -%}
    {%- set result = '_' ~ result -%}
  {%- endif -%}

  {#- A source key can normalize to a bare SQL reserved word (e.g. a JSON key literally
      named "null"), which can't be used unquoted as a column alias. #}
  {%- set reserved_words = [
    'all', 'and', 'any', 'as', 'asc', 'between', 'by', 'case', 'check', 'column',
    'create', 'cross', 'current', 'default', 'delete', 'desc', 'distinct', 'drop',
    'else', 'end', 'exists', 'false', 'for', 'foreign', 'from', 'full', 'group',
    'having', 'in', 'index', 'inner', 'insert', 'intersect', 'into', 'is', 'join',
    'key', 'left', 'like', 'limit', 'not', 'null', 'on', 'or', 'order', 'outer',
    'primary', 'references', 'right', 'select', 'set', 'table', 'then', 'true',
    'union', 'unique', 'update', 'using', 'values', 'when', 'where', 'with'
  ] -%}
  {%- if result in reserved_words -%}
    {%- set result = result ~ '_' -%}
  {%- endif -%}

  {{ return(result) }}
{% endmacro %}
