-- Diccionario de indicadores del Censo Económico.
-- Usa la descripción del año más reciente disponible para cada indicador.
-- Marca los ~15 indicadores clave con es_indicador_clave = true.
with
    diccionario as (select * from {{ ref("stg_ce__diccionarios") }}),

    -- Tomar la descripción del año más reciente por indicador
    con_rango as (
        select
            *, row_number() over (partition by nombre_columna order by anio desc) as rn
        from diccionario
    ),

    mas_reciente as (select * from con_rango where rn = 1),

    -- Indicadores clave para análisis rápido
    indicadores_clave(nombre_columna) as (
        values
            ('ue'),
            ('h001a'),
            ('h000a'),
            ('h010a'),
            ('h020a'),
            ('a111a'),
            ('a121a'),
            ('a131a'),
            ('a211a'),
            ('a221a'),
            ('a700a'),
            ('a800a'),
            ('k000a'),
            ('i000a'),
            ('j000a')
    )

select
    mr.nombre_columna,
    mr.descripcion,
    -- Descripción corta: todo antes del primer paréntesis o dos puntos
    trim(regexp_extract(mr.descripcion, '^([^(:]+)', 1)) as descripcion_corta,
    -- Unidad de medida extraída del paréntesis, ej. "millones de pesos"
    nullif(trim(regexp_extract(mr.descripcion, '\(([^)]+)\)', 1)), '') as unidad,
    mr.tipo_dato,
    mr.anio as anio_descripcion,
    case
        when ic.nombre_columna is not null then true else false
    end as es_indicador_clave
from mas_reciente as mr
left join indicadores_clave as ic on mr.nombre_columna = ic.nombre_columna
order by mr.nombre_columna
