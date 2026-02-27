-- Diccionario de indicadores del Censo de Población.
-- Usa la descripción del año más reciente disponible para cada indicador.
with
    diccionario as (select * from {{ ref("stg_cp__diccionarios") }}),

    con_rango as (
        select
            *, row_number() over (partition by nombre_columna order by anio desc) as rn
        from diccionario
    ),

    mas_reciente as (select * from con_rango where rn = 1)

select nombre_columna, descripcion, tipo_dato, anio as anio_descripcion
from mas_reciente
order by nombre_columna
