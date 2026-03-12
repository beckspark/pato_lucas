-- Datos del Censo Económico con jerarquía SCIAN denormalizada.
-- Incluye nivel_scian derivado del catálogo de actividades.
-- Grano: anio × nivel_geografico × cve_ent × cve_mun × codigo_actividad.
with
    fuente as (select * from {{ ref("stg_ce__datos") }}),

    actividades as (
        select codigo, lower(clasificador) as nivel_scian
        from {{ ref("stg_ce__actividades") }}
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "anio",
                "nivel_geografico",
                "cve_ent",
                "cve_mun",
                "codigo_actividad",
            ]
        )
    }} as censo_economico_sk,

    f.anio,
    f.nivel_geografico,
    f.cve_ent,
    f.cve_mun,
    f.codigo_actividad,

    -- Nivel SCIAN del catálogo de actividades
    a.nivel_scian,

    -- Jerarquía SCIAN denormalizada con sectores compuestos
    case
        when left(f.codigo_actividad, 2) in ('31', '32', '33')
        then '31-33'
        when left(f.codigo_actividad, 2) in ('48', '49')
        then '48-49'
        else left(f.codigo_actividad, 2)
    end as codigo_sector,

    case
        when a.nivel_scian in ('subsector', 'rama', 'subrama', 'clase')
        then left(f.codigo_actividad, 3)
    end as codigo_subsector,

    case
        when a.nivel_scian in ('rama', 'subrama', 'clase')
        then left(f.codigo_actividad, 4)
    end as codigo_rama,

    -- 10 indicadores pivotados
    f.ue,
    f.h001a,
    f.h000a,
    f.i000a,
    f.j000a,
    f.k000a,
    f.a111a,
    f.m000a,
    f.p000a,
    f.q000a

from fuente as f
inner join actividades as a on f.codigo_actividad = a.codigo
