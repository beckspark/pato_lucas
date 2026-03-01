-- Dimensión SCIAN con jerarquía completa para drill-down en BI.
-- Incluye codigo_padre para navegación jerárquica y descripcion_sector para labels.
-- Maneja sectores compuestos del SCIAN (31-33, 48-49).
with
    actividades as (
        select codigo, descripcion, lower(clasificador) as nivel
        from {{ ref("stg_ce__actividades") }}
    ),

    con_sector as (
        select
            a.codigo,
            a.descripcion,
            a.nivel,
            case
                when left(a.codigo, 2) in ('31', '32', '33')
                then '31-33'
                when left(a.codigo, 2) in ('48', '49')
                then '48-49'
                else left(a.codigo, 2)
            end as codigo_sector,
            case
                when a.nivel = 'sector'
                then null
                when a.nivel = 'subsector'
                then
                    case
                        when left(a.codigo, 2) in ('31', '32', '33')
                        then '31-33'
                        when left(a.codigo, 2) in ('48', '49')
                        then '48-49'
                        else left(a.codigo, 2)
                    end
                when a.nivel = 'rama'
                then left(a.codigo, 3)
                when a.nivel = 'subrama'
                then left(a.codigo, 4)
                when a.nivel = 'clase'
                then left(a.codigo, 5)
            end as codigo_padre
        from actividades as a
    )

select
    cs.codigo,
    cs.descripcion,
    cs.nivel,
    cs.codigo_padre,
    cs.codigo_sector,
    sec.descripcion as descripcion_sector
from con_sector as cs
left join actividades as sec on cs.codigo_sector = sec.codigo and sec.nivel = 'sector'
order by cs.codigo
