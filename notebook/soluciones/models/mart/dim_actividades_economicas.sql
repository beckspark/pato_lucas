-- Dimensión SCIAN con jerarquía para drill-down en BI.
-- Incluye codigo_padre para navegación jerárquica y descripcion_sector para labels.
-- Maneja el sector compuesto 31-33 del SCIAN.
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
                        else left(a.codigo, 2)
                    end
                when a.nivel = 'rama'
                then left(a.codigo, 3)
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
