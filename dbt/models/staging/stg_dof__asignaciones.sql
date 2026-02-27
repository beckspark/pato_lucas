-- Staging de asignaciones del DOF: normaliza nombres de entidad a cve_ent
-- mediante un mapeo inline de 32 estados + alias históricos.
-- Las filas que no mapean (junk: totales, auditorías, meses, etc.) quedan con cve_ent
-- NULL.
{%- set normalizar_entidad -%}
regexp_replace(
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                lower(regexp_replace(trim(entidad), '\s+', ' ', 'g')),
                                '^\*+\s*', ''
                            ),
                            'á', 'a'
                        ),
                        'é', 'e'
                    ),
                    'í', 'i'
                ),
                'ó', 'o'
            ),
            'ú', 'u'
        ),
        'ü', 'u'
    ),
    'ñ', 'ni'
)
{%- endset %}

with
    fuente as (select * from {{ source("sieej", "dof_asignaciones") }}),

    -- Mapeo de nombres normalizados de entidad → cve_ent (32 estados + alias)
    mapeo_entidades(nombre_normalizado, cve_ent) as (
        values
            ('aguascalientes', '01'),
            ('baja california sur', '03'),
            ('baja california', '02'),
            ('campeche', '04'),
            ('chiapas', '07'),
            ('chihuahua', '08'),
            ('ciudad de mexico', '09'),
            ('distrito federal', '09'),
            ('coahuila', '05'),
            ('colima', '06'),
            ('durango', '10'),
            ('guanajuato', '11'),
            ('guerrero', '12'),
            ('hidalgo', '13'),
            ('jalisco', '14'),
            ('mexico', '15'),
            ('michoacan', '16'),
            ('morelos', '17'),
            ('nayarit', '18'),
            ('nuevo leon', '19'),
            ('oaxaca', '20'),
            ('puebla', '21'),
            ('queretaro', '22'),
            ('quintana roo', '23'),
            ('san luis potosi', '24'),
            ('sinaloa', '25'),
            ('sonora', '26'),
            ('tabasco', '27'),
            ('tamaulipas', '28'),
            ('tlaxcala', '29'),
            ('veracruz', '30'),
            ('yucatan', '31'),
            ('zacatecas', '32')
    ),

    con_normalizacion as (
        select *, {{ normalizar_entidad }} as entidad_normalizada from fuente
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
            f.diciembre,
            trim(f.url_fuente) as url_fuente,
            trim(f.fecha_publicacion) as fecha_publicacion
        from con_normalizacion as f
        left join mapeo_entidades as m on f.entidad_normalizada = m.nombre_normalizado
    )

select *
from mapeado
