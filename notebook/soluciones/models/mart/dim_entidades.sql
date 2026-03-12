-- Dimensión de 5 entidades federativas del tutorial.
select distinct cve_ent, nombre_entidad
from {{ ref("stg_ce__entidades_municipios") }}
order by cve_ent
