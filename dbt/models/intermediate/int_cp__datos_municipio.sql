-- Totales pre-calculados por INEGI a nivel municipio.
-- Filtra nivel_geografico = 'municipio' (loc='0000', ageb='0000', mza='000').
-- Agrega clave sustituta sobre año × entidad × municipio.
with
    fuente as (
        select * from {{ ref("stg_cp__datos") }} where nivel_geografico = 'municipio'
    )

select
    {{ dbt_utils.generate_surrogate_key(["anio", "cve_ent", "cve_mun"]) }}
    as censo_poblacion_mun_sk,
    *
from fuente
