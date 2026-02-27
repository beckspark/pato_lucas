-- Dimensión de ~2,478 municipios.
-- FK cruzada entre CE y CP a nivel municipal via cvegeo (5 caracteres).
select cvegeo, cve_ent, cve_mun, nombre_entidad, nombre_municipio
from {{ ref("stg_ce__entidades_municipios") }}
order by cvegeo
