-- Filtra asignaciones DOF válidas: solo filas con entidad mapeada,
-- excluyendo totales y consolidados.
-- Agrega clave sustituta sobre año × ramo × anexo × fondo × entidad.
with
    fuente as (
        select *
        from {{ ref("stg_dof__asignaciones") }}
        where cve_ent is not null and es_total = false and es_consolidado = false
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            ["anio", "ramo", "anexo", "fondo", "cve_ent"]
        )
    }} as asignacion_sk, *
from fuente
