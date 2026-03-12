-- Diccionario de indicadores del Censo Económico.
-- Usa la descripción del año más reciente y marca indicadores clave via seed.
with
    diccionario as (
        select lower(nombre_columna) as nombre_columna, descripcion, tipo_dato, anio
        from {{ source("tutorial", "ce_diccionarios_datos") }}
        -- Filtrar filas de metadatos (no son indicadores)
        where
            nombre_columna
            not in ('ENTIDAD', 'MUNICIPIO', 'CODIGO', 'ID_ESTRATO', 'E000')
    ),

    -- Tomar la descripción del año más reciente por indicador
    con_rango as (
        select
            *, row_number() over (partition by nombre_columna order by anio desc) as rn
        from diccionario
    ),

    mas_reciente as (select * from con_rango where rn = 1),

    -- Indicadores clave desde el seed
    clave as (
        select nombre_columna
        from {{ ref("indicadores_clave") }}
        where es_clave = 'true'
    )

select
    mr.nombre_columna,
    mr.descripcion,
    -- Descripción corta: todo antes del primer paréntesis
    trim(regexp_extract(mr.descripcion, '^([^(]+)', 1)) as descripcion_corta,
    -- Unidad de medida extraída del paréntesis
    nullif(trim(regexp_extract(mr.descripcion, '\(([^)]+)\)', 1)), '') as unidad,
    mr.tipo_dato,
    mr.anio as anio_descripcion,
    case
        when ic.nombre_columna is not null then true else false
    end as es_indicador_clave
from mas_reciente as mr
left join clave as ic on mr.nombre_columna = ic.nombre_columna
order by mr.nombre_columna
