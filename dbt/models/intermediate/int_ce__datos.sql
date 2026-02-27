-- Filtro a nivel municipio solamente (excluye totales nacionales y estatales
-- para evitar doble conteo en rollups de BI).
-- Agrega clave sustituta sobre el grano: año × entidad × municipio × actividad ×
-- estrato.
with
    fuente as (
        select * from {{ ref("stg_ce__datos") }} where nivel_geografico = 'municipio'
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            ["anio", "cve_ent", "cve_mun", "codigo_actividad", "id_estrato"]
        )
    }} as censo_economico_sk, *
from fuente
