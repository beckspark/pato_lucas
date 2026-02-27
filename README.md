# Pato Lucas — dbt + DuckDB para insights BI del IIEG

Pipeline de datos que transforma fuentes públicas (INEGI, DOF) en tablas analíticas listas para exploración BI.

## Prerequisitos

- **[uv](https://docs.astral.sh/uv/getting-started/installation/)** — gestor de paquetes Python
- **[Docker](https://docs.docker.com/get-started/get-docker/)** — para el futuro servicio de Superset (aún no implementado)

### Base de datos SIEEJ

Descargar el archivo DuckDB pre-poblado (~1.5 GB) y colocarlo en `data/sieej.duckdb`:

https://drive.google.com/file/d/1ikx-WWrll02h91S6z8vjdSx77R_c4Wbn/view?usp=drive_link

### Instalación

```bash
uv sync
uv run pre-commit install
cd dbt && uv run dbt build
```

## Fuentes de datos

### Censo Económico (INEGI)

Datos de los Censos Económicos de INEGI. Cubre establecimientos productivos a nivel municipio, desglosados por actividad económica (SCIAN) y estrato de tamaño. Incluye indicadores de producción, empleo, inversión y gastos.

### Censo de Población y Vivienda (INEGI)

Datos del Censo de Población y Vivienda 2020 de INEGI. Contiene 222 indicadores demográficos, educativos, de empleo, salud, vivienda y servicios a nivel entidad y municipio.

### Asignaciones Federales (DOF)

Montos publicados en el Diario Oficial de la Federación para fondos federales asignados a entidades. Incluye desglose mensual por ramo, anexo y fondo.

## Tablas de hechos

Todas las tablas de hechos están en formato largo (despivoteado) con columnas legibles joinadas directamente — no requieren joins manuales para exploración BI.

### `fct_censo_economico`

15 indicadores clave despivoteados. Grano: año × entidad × municipio × actividad × estrato × indicador.

Incluye: `nombre_entidad`, `nombre_municipio`, `descripcion_actividad`, `clasificador_actividad`, `descripcion_estrato`, `descripcion` (corta del indicador), `unidad` (ej. "millones de pesos").

| Indicador | Descripción | Unidad |
|-----------|-------------|--------|
| a111a | Producción bruta total | millones de pesos |
| a121a | Consumo intermedio | millones de pesos |
| a131a | Valor agregado censal bruto | millones de pesos |
| a211a | Inversión total | millones de pesos |
| a221a | Formación bruta de capital fijo | millones de pesos |
| a700a | Total de gastos | millones de pesos |
| a800a | Total de ingresos | millones de pesos |
| j000a | Total de remuneraciones | millones de pesos |
| k000a | Total de gastos por consumo de bienes y servicios | millones de pesos |
| h001a | Personal ocupado total | — |
| h000a | Personal dependiente de la razón social total | — |
| h010a | Personal remunerado total | — |
| h020a | Personas propietarias, familiares y otro personal no remunerado total | — |
| i000a | Personal no dependiente de la razón social total | — |
| ue | Clave de la unidad económica | — |

### `fct_censo_poblacion_municipio`

222 indicadores demográficos despivoteados. Grano: año × entidad × municipio × indicador.

Incluye: `nombre_entidad`, `nombre_municipio`, `descripcion`.

### `fct_censo_poblacion_entidad`

Mismos 222 indicadores a nivel entidad. Grano: año × entidad × indicador.

Incluye: `nombre_entidad`, `descripcion`.

### `fct_dof_asignaciones_mensuales`

Asignaciones federales despivoteadas por mes. Grano: año × ramo × anexo × fondo × entidad × mes.

Incluye: `mes_nombre`, `mes_numero` (1-12), `monto`.
