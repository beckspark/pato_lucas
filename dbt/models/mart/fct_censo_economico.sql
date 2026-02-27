-- Censo Económico despivoteado con columnas legibles para BI.
-- Solo 15 indicadores clave. Grano: año × entidad × municipio × actividad × estrato ×
-- indicador.
{{
    config(
        post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_indicador ON {{ this }} (indicador)"
    )
}}

with
    fuente as (select * from {{ ref("int_ce__datos") }}),

    despivoteado as (
        select *
        from
            fuente unpivot (
                valor for indicador in (
                    ue,
                    h001a,
                    h000a,
                    h010a,
                    h020a,
                    a111a,
                    a121a,
                    a131a,
                    a211a,
                    a221a,
                    a700a,
                    a800a,
                    k000a,
                    i000a,
                    j000a
                )
            )
    ),

    diccionario as (
        select nombre_columna, descripcion_corta, unidad
        from {{ ref("dim_ce_indicadores") }}
        where es_indicador_clave = true
    ),

    entidades as (select cve_ent, nombre_entidad from {{ ref("dim_entidades") }}),

    municipios as (select cvegeo, nombre_municipio from {{ ref("dim_municipios") }}),

    actividades as (
        select codigo, descripcion, clasificador
        from {{ ref("dim_actividades_economicas") }}
    ),

    estratos as (select id_estrato, descripcion from {{ ref("dim_estratos") }})

select
    d.anio,
    d.cve_ent,
    ent.nombre_entidad,
    d.cve_mun,
    mun.nombre_municipio,
    d.cve_ent || d.cve_mun as cvegeo_municipio,
    d.codigo_actividad,
    act.descripcion as descripcion_actividad,
    act.clasificador as clasificador_actividad,
    d.id_estrato,
    est.descripcion as descripcion_estrato,
    d.indicador,
    dic.descripcion_corta as descripcion,
    dic.unidad,
    d.valor
from despivoteado as d
left join diccionario as dic on d.indicador = dic.nombre_columna
left join entidades as ent on d.cve_ent = ent.cve_ent
left join municipios as mun on (d.cve_ent || d.cve_mun) = mun.cvegeo
left join actividades as act on d.codigo_actividad = act.codigo
left join estratos as est on d.id_estrato = est.id_estrato
order by d.indicador, d.cve_ent, d.anio
