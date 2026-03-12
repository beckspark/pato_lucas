-- Filtra presupuesto DOF válido: solo filas con entidad mapeada,
-- excluyendo totales y consolidados.
-- Agrega clave sustituta sobre año × ramo × anexo × fondo × entidad.
with
    fuente as (
        select *
        from {{ ref("stg_dof__presupuesto") }}
        where cve_ent is not null and es_total = false and es_consolidado = false
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            ["anio", "ramo", "anexo", "fondo", "cve_ent"]
        )
    }} as presupuesto_sk, *
from fuente
