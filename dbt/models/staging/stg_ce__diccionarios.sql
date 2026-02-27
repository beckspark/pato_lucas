-- Staging del diccionario de datos del Censo Económico.
-- Filtra solo filas de indicadores (excluye metadatos como ENTIDAD, MUNICIPIO,
-- CODIGO, etc.)
-- y normaliza nombre_columna a minúsculas.
with
    fuente as (select * from {{ source("sieej", "ce_diccionarios_datos") }}),

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
                'municipio',
                'codigo',
                'id_estrato',
                'sector',
                'subsector',
                'rama',
                'subrama',
                'clase',
                'cvegeo',
                'entidad_federativa',
                'clave_municipio',
                'anio_censal'
            )
            -- Excluir también clasificadores SCIAN (códigos tipo E01, E02, etc.)
            and lower(trim(nombre_columna)) not like 'e%'
    )

select *
from indicadores
