-- Totales pre-calculados por INEGI a nivel entidad.
-- Filtra nivel_geografico = 'entidad' (mun='000', loc='0000').
-- Agrega clave sustituta sobre año × entidad.
with
    fuente as (
        select * from {{ ref("stg_cp__datos") }} where nivel_geografico = 'entidad'
    )

select
    {{ dbt_utils.generate_surrogate_key(["anio", "cve_ent"]) }}
    as censo_poblacion_ent_sk,
    *
from fuente
