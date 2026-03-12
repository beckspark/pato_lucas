-- Staging del Censo Económico: limpieza de strings, clasificación de nivel geográfico,
-- y paso de los 10 indicadores sin transformación.
with
    fuente as (select * from {{ source("tutorial", "ce_datos") }}),

    limpio as (
        select
            id as id_fuente,
            anio,
            nullif(trim(entidad), '') as cve_ent,
            nullif(trim(municipio), '') as cve_mun,
            nullif(trim(codigo), '') as codigo_actividad,
            case
                when trim(entidad) = '' and trim(municipio) = ''
                then 'nacional'
                when trim(entidad) <> '' and trim(municipio) = ''
                then 'entidad'
                when trim(entidad) <> '' and trim(municipio) <> ''
                then 'municipio'
            end as nivel_geografico,

            -- 10 indicadores: se pasan tal cual
            ue,
            h001a,
            h000a,
            i000a,
            j000a,
            k000a,
            a111a,
            m000a,
            p000a,
            q000a
        from fuente
    )

select *
from limpio
