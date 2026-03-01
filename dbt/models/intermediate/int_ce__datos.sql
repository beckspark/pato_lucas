-- Datos del Censo Económico con todos los niveles geográficos y jerarquía SCIAN.
-- Incluye nivel_scian derivado del catálogo de actividades y jerarquía denormalizada
-- (codigo_sector, codigo_subsector, codigo_rama, codigo_subrama).
-- Los 98 indicadores permanecen pivotados como columnas.
-- Grano: anio × nivel_geografico × cve_ent × cve_mun × codigo_actividad × id_estrato.
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
                "id_estrato",
            ]
        )
    }} as censo_economico_sk,

    f.anio,
    f.nivel_geografico,
    f.cve_ent,
    f.cve_mun,
    f.codigo_actividad,
    f.id_estrato,

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

    case
        when a.nivel_scian in ('subrama', 'clase') then left(f.codigo_actividad, 5)
    end as codigo_subrama,

    -- 98 indicadores pivotados
    f.ue,
    f.h001a,
    f.h000a,
    f.h010a,
    f.h020a,
    f.i000a,
    f.j000a,
    f.k000a,
    f.m000a,
    f.a111a,
    f.a121a,
    f.a131a,
    f.a211a,
    f.a221a,
    f.p000c,
    f.q000a,
    f.q000b,
    f.a700a,
    f.a800a,
    f.q000c,
    f.q000d,
    f.p000a,
    f.p000b,
    f.o010a,
    f.o020a,
    f.m700a,
    f.p030c,
    f.a511a,
    f.m020a,
    f.m050a,
    f.m091a,
    f.h001b,
    f.h001c,
    f.h001d,
    f.h000b,
    f.h000c,
    f.h000d,
    f.h010b,
    f.h010c,
    f.h010d,
    f.h101a,
    f.h101b,
    f.h101c,
    f.h101d,
    f.h203a,
    f.h203b,
    f.h203c,
    f.h203d,
    f.h020b,
    f.h020c,
    f.h020d,
    f.i000b,
    f.i000c,
    f.i000d,
    f.i100a,
    f.i100b,
    f.i100c,
    f.i100d,
    f.i200a,
    f.i200b,
    f.i200c,
    f.i200d,
    f.j010a,
    f.j203a,
    f.j300a,
    f.j400a,
    f.j500a,
    f.j600a,
    f.k010a,
    f.k020a,
    f.k030a,
    f.k311a,
    f.k042a,
    f.k412a,
    f.k050a,
    f.k610a,
    f.k620a,
    f.k060a,
    f.k070a,
    f.k810a,
    f.k820a,
    f.k910a,
    f.k950a,
    f.k096a,
    f.k976a,
    f.k090a,
    f.m010a,
    f.m030a,
    f.m090a,
    f.p100a,
    f.p100b,
    f.p030a,
    f.p030b,
    f.q010a,
    f.q020a,
    f.q030a,
    f.q400a,
    f.q900a

from fuente as f
inner join actividades as a on f.codigo_actividad = a.codigo
