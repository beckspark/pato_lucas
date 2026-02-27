-- Staging del catálogo de estratos de tamaño empresarial.
with fuente as (select * from {{ source("sieej", "ce_catalogos_estratos") }})

select trim(id_estrato) as id_estrato, trim(descripcion) as descripcion
from fuente
