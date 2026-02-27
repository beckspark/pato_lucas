-- Staging del catálogo geográfico de entidades y municipios.
-- Limpieza de strings y selección de columnas relevantes.
with
    fuente as (select * from {{ source("sieej", "ce_catalogos_entidades_municipios") }})

select
    trim(cvegeo) as cvegeo,
    trim(cve_ent) as cve_ent,
    trim(nombre_entidad) as nombre_entidad,
    trim(cve_mun) as cve_mun,
    trim(nombre_municipio) as nombre_municipio
from fuente
