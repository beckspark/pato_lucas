# Pato Lucas — dbt + DuckDB para insights BI del IIEG

Pipeline de datos que transforma fuentes públicas (INEGI, DOF) en tablas analíticas listas para exploración BI.

## Prerequisitos

- **[uv](https://docs.astral.sh/uv/getting-started/installation/)** — gestor de paquetes Python
- **[Docker](https://docs.docker.com/get-started/get-docker/)** — para el servicio de Superset
- `make` si quieres utilizar el `Makefile`

### Base de datos SIEEJ

Descargar el archivo DuckDB pre-poblado (~1.5 GB) y colocarlo en `data/sieej.duckdb`:

https://drive.google.com/file/d/1ikx-WWrll02h91S6z8vjdSx77R_c4Wbn/view?usp=drive_link

### Instalación

Para empezar:
```bash
uv sync
uv run pre-commit install
```

luego, hay dos opciones:

#### Opcion 1 -- manual

```bash
cd dbt && uv run dbt deps && uv run dbt build
```

## dbt docs
Se puede auto-servir una instancia de los dbt docs en puerto 8081 via `make docs`

## Superset (exploración BI)

```bash
docker compose up -d
```

Acceder a http://localhost:8089 (usuario: `admin`, contraseña: `admin`).

La conexión a DuckDB (base "SIEEJ") se registra automáticamente al arrancar.
Las tablas mart están disponibles en SQL Lab bajo el schema `mart`.

#### Opcion 2 -- con `make`
Para correr todo de `dbt build` a hacer los dbt docs y el superset, puedes usar:

```bash
make up
```

Si el puerto 8089 u 8081 esta ocupado, se detecta automaticamente uno libre.
Para forzar un puerto especifico:

```bash
SUPERSET_PORT=9090 make up
```

`make down` para desactivar el docker

## Fuentes de datos

### Censo Económico (INEGI)

Datos de los Censos Económicos de INEGI. Cubre establecimientos productivos a nivel municipio, desglosados por actividad económica (SCIAN) y estrato de tamaño. Incluye indicadores de producción, empleo, inversión y gastos.

### Censo de Población y Vivienda (INEGI)

Datos del Censo de Población y Vivienda 2020 de INEGI. Contiene 222 indicadores demográficos, educativos, de empleo, salud, vivienda y servicios a nivel entidad y municipio.

### Asignaciones Federales (DOF)

Montos publicados en el Diario Oficial de la Federación para fondos federales asignados a entidades. Incluye desglose mensual por ramo, anexo y fondo.
