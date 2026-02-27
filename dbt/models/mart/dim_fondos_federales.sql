-- Dimensión de fondos federales: combinaciones únicas de ramo × anexo × fondo.
-- Incluye etiqueta tipo_ramo para los ramos más comunes.
with
    fondos as (
        select distinct ramo, anexo, fondo
        from {{ ref("int_dof__asignaciones") }}
        where ramo is not null
    )

select
    ramo,
    anexo,
    fondo,
    case
        when ramo = 28
        then 'Participaciones'
        when ramo = 33
        then 'Aportaciones'
        when ramo = 23
        then 'Provisiones salariales y económicas'
        else 'Otro (ramo ' || ramo || ')'
    end as tipo_ramo
from fondos
order by ramo, anexo, fondo
