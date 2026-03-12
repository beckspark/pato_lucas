-- Macro para normalizar texto: minúsculas, sin acentos, sin asteriscos,
-- espacios normalizados, ñ → ni.
-- Uso: {{ normalizar_texto('nombre_columna') }}
{% macro normalizar_texto(columna) %}
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    lower(
                                        regexp_replace(
                                            trim({{ columna }}), '\s+', ' ', 'g'
                                        )
                                    ),
                                    '^\*+\s*',
                                    ''
                                ),
                                'á',
                                'a'
                            ),
                            'é',
                            'e'
                        ),
                        'í',
                        'i'
                    ),
                    'ó',
                    'o'
                ),
                'ú',
                'u'
            ),
            'ü',
            'u'
        ),
        'ñ',
        'ni'
    )
{% endmacro %}
