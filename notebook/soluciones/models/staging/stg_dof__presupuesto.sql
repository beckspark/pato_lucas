-- Staging del presupuesto DOF: normaliza nombres de entidad a cve_ent
-- mediante la macro normalizar_texto y un mapeo inline de 5 estados.
-- Las filas que no mapean (junk: totales, consolidados) quedan con cve_ent NULL.
with
    fuente as (select * from {{ source("tutorial", "dof_presupuesto") }}),

    -- Mapeo de nombres normalizados de entidad → cve_ent
    mapeo_entidades(nombre_normalizado, cve_ent) as (
        values
            ('ciudad de mexico', '09'),
            ('guanajuato', '11'),
            ('jalisco', '14'),
            ('nuevo leon', '19'),
            ('puebla', '21')
    ),

    con_normalizacion as (
        select *, {{ normalizar_texto("entidad") }} as entidad_normalizada from fuente
    ),

    mapeado as (
        select
            f.id as id_fuente,
            f.anio,
            f.ramo,
            f.anexo,
            trim(f.fondo) as fondo,
            trim(f.entidad) as entidad_original,
            f.entidad_normalizada,
            m.cve_ent,
            f.es_total,
            f.es_consolidado,
            f.anual,
            f.enero,
            f.febrero,
            f.marzo,
            f.abril,
            f.mayo,
            f.junio,
            f.julio,
            f.agosto,
            f.septiembre,
            f.octubre,
            f.noviembre,
            f.diciembre
        from con_normalizacion as f
        left join mapeo_entidades as m on f.entidad_normalizada = m.nombre_normalizado
    )

select *
from mapeado
