-- Dimensión de 32 entidades federativas.
-- FK cruzada entre las tres fuentes (CE, CP, DOF) a nivel estatal.
select distinct cve_ent, nombre_entidad
from {{ ref("stg_ce__entidades_municipios") }}
order by cve_ent
