-- Censo de Población despivoteado a nivel municipio con columnas legibles para BI.
-- 222 indicadores. Grano: año × entidad × municipio × indicador.
{{
    config(
        post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_indicador ON {{ this }} (indicador)"
    )
}}

with
    fuente as (select * from {{ ref("int_cp__datos_municipio") }}),

    despivoteado as (
        select *
        from
            fuente unpivot (
                valor for indicador in (
                    columns(
                        * exclude (
                            censo_poblacion_mun_sk,
                            id_fuente,
                            anio,
                            cve_ent,
                            cve_mun,
                            cve_loc,
                            cve_ageb,
                            cve_mza,
                            nombre_entidad,
                            nombre_municipio,
                            nombre_localidad,
                            cvegeo,
                            nivel_geografico
                        )
                    )
                )
            )
    ),

    diccionario as (
        select nombre_columna, descripcion from {{ ref("dim_cp_indicadores") }}
    ),

    entidades as (select cve_ent, nombre_entidad from {{ ref("dim_entidades") }}),

    municipios as (select cvegeo, nombre_municipio from {{ ref("dim_municipios") }})

select
    d.anio,
    d.cve_ent,
    ent.nombre_entidad,
    d.cve_mun,
    mun.nombre_municipio,
    d.cve_ent || d.cve_mun as cvegeo_municipio,
    d.indicador,
    dic.descripcion,
    d.valor
from despivoteado as d
left join diccionario as dic on d.indicador = dic.nombre_columna
left join entidades as ent on d.cve_ent = ent.cve_ent
left join municipios as mun on (d.cve_ent || d.cve_mun) = mun.cvegeo
order by d.indicador, d.cve_ent, d.anio
