-- Asignaciones federales despivoteadas por mes.
-- Grano: año × ramo × anexo × fondo × entidad × mes.
-- ~200K filas (12 meses × 16,636 registros fuente).
with
    fuente as (select * from {{ ref("fct_asignaciones_federales") }}),

    despivoteado as (
        select *
        from
            fuente unpivot (
                monto for mes_nombre in (
                    enero,
                    febrero,
                    marzo,
                    abril,
                    mayo,
                    junio,
                    julio,
                    agosto,
                    septiembre,
                    octubre,
                    noviembre,
                    diciembre
                )
            )
    )

select
    d.anio,
    d.cve_ent,
    e.nombre_entidad,
    d.ramo,
    d.anexo,
    d.fondo,
    ff.tipo_ramo,
    d.mes_nombre,
    -- Número de mes para ordenar cronológicamente
    case
        d.mes_nombre
        when 'enero'
        then 1
        when 'febrero'
        then 2
        when 'marzo'
        then 3
        when 'abril'
        then 4
        when 'mayo'
        then 5
        when 'junio'
        then 6
        when 'julio'
        then 7
        when 'agosto'
        then 8
        when 'septiembre'
        then 9
        when 'octubre'
        then 10
        when 'noviembre'
        then 11
        when 'diciembre'
        then 12
    end as mes_numero,
    d.monto
from despivoteado as d
left join {{ ref("dim_entidades") }} as e on d.cve_ent = e.cve_ent
left join
    {{ ref("dim_fondos_federales") }} as ff
    on d.ramo = ff.ramo
    and d.anexo = ff.anexo
    and d.fondo = ff.fondo
order by d.anio, d.cve_ent, d.ramo, mes_numero
