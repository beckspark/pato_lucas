-- Staging del catálogo de actividades económicas SCIAN.
-- Limpieza de strings y paso de campos.
with fuente as (select * from {{ source("sieej", "ce_catalogos_actividades") }})

select
    trim(codigo) as codigo,
    trim(descripcion) as descripcion,
    trim(clasificador) as clasificador
from fuente
