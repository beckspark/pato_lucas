-- Staging del diccionario de datos del Censo de Población.
-- Filtra solo filas de indicadores (excluye metadatos geográficos)
-- y normaliza nombre_columna a minúsculas.
with
    fuente as (select * from {{ source("sieej", "cp_diccionarios_datos") }}),

    indicadores as (
        select
            anio,
            lower(trim(nombre_columna)) as nombre_columna,
            trim(descripcion) as descripcion,
            trim(tipo_dato) as tipo_dato,
            trim(longitud) as longitud,
            trim(codigos_validos) as codigos_validos
        from fuente
        where
            lower(trim(nombre_columna)) not in (
                'entidad',
                'nom_ent',
                'mun',
                'nom_mun',
                'loc',
                'nom_loc',
                'ageb',
                'mza',
                'cvegeo'
            )
    )

select *
from indicadores
