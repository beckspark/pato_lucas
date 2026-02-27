-- Dimensión de actividades económicas SCIAN con jerarquía padre para drill-down en BI.
-- codigo_padre se obtiene truncando el último nivel de la jerarquía SCIAN.
with actividades as (select * from {{ ref("stg_ce__actividades") }})

select
    codigo,
    descripcion,
    clasificador,
    case
        when clasificador = 'Sector'
        then null
        when clasificador = 'Subsector'
        then left(codigo, 2)
        when clasificador = 'Rama'
        then left(codigo, 3)
        when clasificador = 'Subrama'
        then left(codigo, 4)
        when clasificador = 'Clase'
        then left(codigo, 5)
    end as codigo_padre
from actividades
order by codigo
