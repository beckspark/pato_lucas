#!/usr/bin/env python3
"""Provisiona el dashboard de Censos Economicos via la API REST de Superset.

Idempotente — seguro de ejecutar multiples veces. Verifica recursos existentes antes de crear.

Arquitectura "switchboard": los 98 indicadores permanecen pivotados en DuckDB.
Un virtual dataset SQL con UNPIVOT + COLUMNS(* EXCLUDE) despivotea todos los
indicadores; un JOIN a dim_ce_indicadores provee nombres legibles para el filtro.

Disenado para ejecutarse como proceso background dentro del contenedor,
esperando a que la API este lista antes de provisionar.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Datasets
DATASET_SCHEMA = "sieej.mart"
DATASET_VIRTUAL_NAME = "ce_switchboard"

DASHBOARD_SLUG = "censos-economicos"
DASHBOARD_TITLE = "Censos Economicos"

# Nombres de charts
HEADER_CHART_NAME = "CE -- Titulo dinamico"
BIG_NUMBER_CHART_NAME = "CE -- Total"
TREEMAP_CHART_NAME = "CE -- Sectores economicos"
SUMMARY_TABLE_CHART_NAME = "CE -- Resumen sectorial"
DETAIL_TABLE_CHART_NAME = "CE -- Detalle SCIAN"
BAR_CHART_NAME = "CE -- Municipios por indicador"

# SQL del virtual dataset ce_switchboard.
# UNPIVOT de todos los indicadores con COLUMNS(* EXCLUDE) — sin Jinja ni dict.
# La cross-referencia a dim_ce_indicadores provee descripcion_corta legible;
# el filtro nativo de Superset aplica WHERE descripcion_corta IN (...) directamente.
SWITCHBOARD_SQL = """\
SELECT
    f.anio,
    f.nivel_geografico,
    f.cve_ent,
    e.nombre_entidad,
    f.cve_mun,
    m.nombre_municipio,
    f.codigo_actividad,
    s.descripcion AS descripcion_actividad,
    CASE f.nivel_scian
        WHEN 'sector' THEN '1. Sector'
        WHEN 'subsector' THEN '2. Subsector'
        WHEN 'rama' THEN '3. Rama'
        WHEN 'subrama' THEN '4. Subrama'
        WHEN 'clase' THEN '5. Clase'
    END AS nivel_scian,
    f.codigo_sector,
    sec.descripcion AS descripcion_sector,
    ind.descripcion_corta,
    ind.unidad AS unidad_indicador,
    f.indicador_col AS indicador,
    f.valor
FROM (
    SELECT *
    FROM sieej.mart.fct_censo_economico
    UNPIVOT (
        valor FOR indicador_col IN (
            COLUMNS(* EXCLUDE (
                censo_economico_sk, anio, nivel_geografico, cve_ent, cve_mun,
                codigo_actividad, id_estrato, nivel_scian, codigo_sector,
                codigo_subsector, codigo_rama, codigo_subrama
            ))
        )
    )
) AS f
LEFT JOIN sieej.mart.dim_entidades AS e ON f.cve_ent = e.cve_ent
LEFT JOIN sieej.mart.dim_municipios AS m ON (f.cve_ent || f.cve_mun) = m.cvegeo
LEFT JOIN sieej.mart.dim_scian AS s ON f.codigo_actividad = s.codigo
LEFT JOIN sieej.mart.dim_scian AS sec ON f.codigo_sector = sec.codigo AND sec.nivel = 'sector'
LEFT JOIN sieej.mart.dim_ce_indicadores AS ind ON f.indicador_col = ind.nombre_columna
"""

BASE_URL = "http://localhost:8088"
MAX_INTENTOS = 30
INTERVALO_RETRY = 2  # segundos


# ---------------------------------------------------------------------------
# Cliente HTTP inline (usa requests, ya incluido en la imagen de Superset)
# ---------------------------------------------------------------------------


class SupersetClient:
    """Cliente ligero para la API REST de Superset con autenticacion JWT + CSRF."""

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._access_token: str = ""
        self._csrf_token: str = ""

        self._login(username, password)
        self._fetch_csrf()

    # -- Autenticacion -------------------------------------------------------

    def _login(self, username: str, password: str) -> None:
        resp = self._session.post(
            f"{self.base_url}/api/v1/security/login",
            json={"username": username, "password": password, "provider": "db"},
        )
        resp.raise_for_status()
        self._access_token = resp.json()["access_token"]

    def _fetch_csrf(self) -> None:
        resp = self._session.get(
            f"{self.base_url}/api/v1/security/csrf_token/",
            headers={"Authorization": f"Bearer {self._access_token}"},
        )
        resp.raise_for_status()
        self._csrf_token = resp.json()["result"]

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "X-CSRFToken": self._csrf_token,
            "Referer": self.base_url,
            "Content-Type": "application/json",
        }

    # -- Metodos HTTP --------------------------------------------------------

    def get(self, path: str, **kwargs: Any) -> requests.Response:
        resp = self._session.get(f"{self.base_url}{path}", headers=self._headers(), **kwargs)
        resp.raise_for_status()
        return resp

    def post(self, path: str, **kwargs: Any) -> requests.Response:
        resp = self._session.post(f"{self.base_url}{path}", headers=self._headers(), **kwargs)
        resp.raise_for_status()
        return resp

    def put(self, path: str, **kwargs: Any) -> requests.Response:
        resp = self._session.put(f"{self.base_url}{path}", headers=self._headers(), **kwargs)
        resp.raise_for_status()
        return resp

    def close(self) -> None:
        self._session.close()


# ---------------------------------------------------------------------------
# Espera activa — la API debe estar lista antes de provisionar
# ---------------------------------------------------------------------------


def _esperar_api(base_url: str) -> None:
    """Espera hasta que el endpoint /health responda 200."""
    for intento in range(1, MAX_INTENTOS + 1):
        try:
            resp = requests.get(f"{base_url}/health", timeout=5)
            if resp.status_code == 200:
                print(f"    API lista (intento {intento}/{MAX_INTENTOS}).")
                return
        except requests.RequestException:
            pass
        print(f"    Esperando API... (intento {intento}/{MAX_INTENTOS})")
        time.sleep(INTERVALO_RETRY)
    print("ERROR: La API de Superset no respondio a tiempo.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def find_database_id(client: SupersetClient, name: str = "SIEEJ") -> int:
    """Retorna el id de la base de datos con el nombre dado."""
    resp = client.get(
        "/api/v1/database/",
        params={"q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": name}]})},
    )
    results = resp.json().get("result", [])
    if not results:
        print(f"ERROR: Base de datos '{name}' no encontrada. Verifica registrar_duckdb.py.")
        sys.exit(1)
    db_id: int = results[0]["id"]
    return db_id


def find_existing(client: SupersetClient, endpoint: str, filter_col: str, filter_val: str) -> int | None:
    """Retorna el id de un recurso existente, o None."""
    resp = client.get(
        endpoint,
        params={"q": json.dumps({"filters": [{"col": filter_col, "opr": "eq", "value": filter_val}]})},
    )
    results = resp.json().get("result", [])
    if results:
        return results[0]["id"]  # type: ignore[no-any-return]
    return None


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


def create_virtual_dataset(client: SupersetClient, db_id: int) -> int:
    """Crea el virtual dataset ce_switchboard (UNPIVOT + JOINs a dimensiones)."""
    existing = find_existing(client, "/api/v1/dataset/", "table_name", DATASET_VIRTUAL_NAME)
    if existing:
        print(f"    Dataset '{DATASET_VIRTUAL_NAME}' ya existe (id={existing}). Actualizando SQL...")
        client.put(
            f"/api/v1/dataset/{existing}",
            json={"sql": SWITCHBOARD_SQL, "is_managed_externally": False},
        )
        return existing

    resp = client.post(
        "/api/v1/dataset/",
        json={
            "database": db_id,
            "table_name": DATASET_VIRTUAL_NAME,
            "schema": DATASET_SCHEMA,
            "sql": SWITCHBOARD_SQL,
        },
    )
    ds_id: int = resp.json()["id"]
    print(f"    Dataset '{DATASET_VIRTUAL_NAME}' creado (id={ds_id}).")
    return ds_id


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def _metric_sum_valor() -> dict[str, Any]:
    """Metrica reutilizable: SUM(valor)."""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": "valor", "type": "FLOAT"},
        "aggregate": "SUM",
        "label": "SUM(valor)",
    }


def _adhoc_filter_nivel_scian_sector() -> dict[str, Any]:
    """Filtro adhoc fijo: nivel_scian = '1. Sector'."""
    return {
        "expressionType": "SIMPLE",
        "clause": "WHERE",
        "subject": "nivel_scian",
        "operator": "==",
        "comparator": "1. Sector",
        "filterOptionName": f"filter_{uuid.uuid4().hex[:8]}",
    }


def _upsert_chart(
    client: SupersetClient,
    name: str,
    dataset_id: int,
    viz_type: str,
    params: dict[str, Any],
) -> int:
    """Crea o actualiza un chart por nombre."""
    existing = find_existing(client, "/api/v1/chart/", "slice_name", name)
    payload = {
        "slice_name": name,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(params),
    }
    if existing:
        print(f"    Chart '{name}' ya existe (id={existing}). Actualizando...")
        client.put(f"/api/v1/chart/{existing}", json=payload)
        return existing

    resp = client.post("/api/v1/chart/", json=payload)
    chart_id: int = resp.json()["id"]
    print(f"    Chart '{name}' creado (id={chart_id}).")
    return chart_id


def create_header_chart(client: SupersetClient, dataset_id: int) -> int:
    template = (
        '<div style="padding:4px 0">'
        '<h2 style="margin:0">Censos Economicos</h2>'
        '<h3 style="margin:0;color:#aaa">'
        "{{#with (lookup data 0)}}{{nombre_entidad}}{{/with}}"
        " -- "
        "{{#each data}}{{#unless @first}}, {{/unless}}{{descripcion_corta}}{{/each}}"
        "</h3>"
        "</div>"
    )
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "handlebars",
        "query_mode": "aggregate",
        "groupby": ["nombre_entidad", "descripcion_corta"],
        "metrics": [],
        "row_limit": 50,
        "order_desc": True,
        "handlebarsTemplate": template,
        "styleTemplate": "",
        "adhoc_filters": [],
    }
    return _upsert_chart(client, HEADER_CHART_NAME, dataset_id, "handlebars", params)


def create_big_number_chart(client: SupersetClient, dataset_id: int) -> int:
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "big_number_total",
        "metric": _metric_sum_valor(),
        "y_axis_format": "SMART_NUMBER",
        "subtitle": "valor total",
        "adhoc_filters": [],
    }
    return _upsert_chart(client, BIG_NUMBER_CHART_NAME, dataset_id, "big_number_total", params)


def create_treemap_chart(client: SupersetClient, dataset_id: int) -> int:
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "treemap_v2",
        "groupby": ["descripcion_sector"],
        "metric": _metric_sum_valor(),
        "show_labels": True,
        "show_upper_labels": True,
        "number_format": "SMART_NUMBER",
        "row_limit": 50,
        "color_scheme": "supersetColors",
        "adhoc_filters": [_adhoc_filter_nivel_scian_sector()],
    }
    return _upsert_chart(client, TREEMAP_CHART_NAME, dataset_id, "treemap_v2", params)


def create_summary_table_chart(client: SupersetClient, dataset_id: int) -> int:
    """Tabla de resumen sectorial — siempre a nivel sector (incluye valores enmascarados)."""
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "query_mode": "aggregate",
        "groupby": ["nivel_scian", "descripcion_sector", "descripcion_corta"],
        "metrics": [_metric_sum_valor()],
        "row_limit": 1000,
        "server_pagination": True,
        "server_page_length": 100,
        "order_desc": True,
        "include_search": True,
        "adhoc_filters": [_adhoc_filter_nivel_scian_sector()],
    }
    return _upsert_chart(client, SUMMARY_TABLE_CHART_NAME, dataset_id, "table", params)


def create_detail_table_chart(client: SupersetClient, dataset_id: int) -> int:
    """Tabla de detalle SCIAN — respeta el filtro de nivel SCIAN del dashboard."""
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "query_mode": "aggregate",
        "groupby": ["nivel_scian", "descripcion_actividad", "descripcion_corta"],
        "metrics": [_metric_sum_valor()],
        "row_limit": 1000,
        "server_pagination": True,
        "server_page_length": 100,
        "order_desc": True,
        "include_search": True,
        "adhoc_filters": [],
    }
    return _upsert_chart(client, DETAIL_TABLE_CHART_NAME, dataset_id, "table", params)


def create_bar_chart(client: SupersetClient, dataset_id: int) -> int:
    viz = "echarts_timeseries_bar"
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz,
        "x_axis": "nombre_municipio",
        "time_grain_sqla": "P1D",
        "metrics": [_metric_sum_valor()],
        "groupby": [],
        "order_desc": True,
        "row_limit": 200,
        "color_scheme": "supersetColors",
        "show_legend": False,
        "x_axis_sort_asc": False,
        "x_axis_sort_series": "sum",
        "x_axis_sort_series_ascending": False,
        "truncate_metric": True,
        "show_empty_columns": False,
        "y_axis_format": "SMART_NUMBER",
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "orientation": "vertical",
        "adhoc_filters": [],
    }
    return _upsert_chart(client, BAR_CHART_NAME, dataset_id, viz, params)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


def _stable_filter_id(name: str) -> str:
    """ID de filtro deterministico a partir del nombre — sobrevive re-provisiones."""
    raw = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"censos-economicos.{name}")).upper()
    return f"NATIVE_FILTER-{raw}"


def _build_native_filters(
    switchboard_ds_id: int,
    *,
    excluir_nivel_scian: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Construye la configuracion de los 7 filtros nativos (sin estrato).

    excluir_nivel_scian: IDs de charts que deben ignorar el filtro Nivel SCIAN
    (treemap y resumen sectorial, que tienen adhoc_filter fijo a '1. Sector').
    """
    f_anio_id = _stable_filter_id("anio")
    f_nivel_geo_id = _stable_filter_id("nivel_geografico")
    f_estado_id = _stable_filter_id("estado")
    f_indicador_id = _stable_filter_id("indicador")
    f_nivel_scian_id = _stable_filter_id("nivel_scian")
    f_mun_id = _stable_filter_id("municipio")
    f_sector_id = _stable_filter_id("sector")

    def _filter(
        fid: str,
        name: str,
        column: str,
        dataset_id: int,
        *,
        multi: bool = False,
        cascade_from: str | list[str] | None = None,
        enable_empty: bool = False,
        search_all: bool = True,
        default_values: list[Any] | None = None,
        scope_excluded: list[int] | None = None,
    ) -> dict[str, Any]:
        if isinstance(cascade_from, list):
            parent_ids = cascade_from
        elif cascade_from:
            parent_ids = [cascade_from]
        else:
            parent_ids = []
        f: dict[str, Any] = {
            "id": fid,
            "type": "NATIVE_FILTER",
            "name": name,
            "description": "",
            "filterType": "filter_select",
            "targets": [{"datasetId": dataset_id, "column": {"name": column}}],
            "defaultDataMask": {"filterState": {}, "extraFormData": {}, "ownState": {}},
            "cascadeParentIds": parent_ids,
            "scope": {"rootPath": ["ROOT_ID"], "excluded": scope_excluded or []},
            "controlValues": {
                "enableEmptyFilter": enable_empty,
                "defaultToFirstItem": False,
                "multiSelect": multi,
                "searchAllOptions": search_all,
                "inverseSelection": False,
            },
        }
        if default_values is not None:
            f["defaultDataMask"] = {
                "filterState": {"value": default_values},
                "extraFormData": {
                    "filters": [{"col": column, "op": "IN", "val": default_values}],
                },
                "ownState": {},
            }
        return f

    filters = [
        # 1. Anio
        _filter(
            f_anio_id,
            "Anio",
            "anio",
            switchboard_ds_id,
            multi=True,
            default_values=[2024],
        ),
        # 2. Nivel Geografico
        _filter(
            f_nivel_geo_id,
            "Nivel Geografico",
            "nivel_geografico",
            switchboard_ds_id,
            default_values=["municipio"],
        ),
        # 3. Estado (cascada desde Nivel Geografico)
        _filter(
            f_estado_id,
            "Estado",
            "nombre_entidad",
            switchboard_ds_id,
            cascade_from=f_nivel_geo_id,
            default_values=["Jalisco"],
        ),
        # 4. Indicador (descripcion legible via cross-ref a dim_ce_indicadores)
        _filter(
            f_indicador_id,
            "Indicador",
            "descripcion_corta",
            switchboard_ds_id,
            multi=True,
            default_values=["Clave de la unidad económica"],
        ),
        # 5. Nivel SCIAN (valores con prefijo numerico para orden correcto)
        # Excluye treemap y resumen sectorial — tienen adhoc_filter fijo a '1. Sector'
        _filter(
            f_nivel_scian_id,
            "Nivel SCIAN",
            "nivel_scian",
            switchboard_ds_id,
            default_values=["1. Sector"],
            scope_excluded=excluir_nivel_scian or [],
        ),
        # 6. Municipio (cascada desde Estado)
        _filter(
            f_mun_id,
            "Municipio",
            "nombre_municipio",
            switchboard_ds_id,
            multi=True,
            cascade_from=f_estado_id,
        ),
        # 7. Sector
        _filter(
            f_sector_id,
            "Sector",
            "descripcion_sector",
            switchboard_ds_id,
            multi=True,
        ),
    ]
    return filters


def _build_position_json(
    header_id: int,
    big_number_id: int,
    treemap_id: int,
    summary_table_id: int,
    detail_table_id: int,
    bar_id: int,
) -> dict[str, Any]:
    """Construye el JSON de posiciones del layout del dashboard."""
    header_key = f"CHART-header-{header_id}"
    big_number_key = f"CHART-bignumber-{big_number_id}"
    treemap_key = f"CHART-treemap-{treemap_id}"
    summary_key = f"CHART-summary-{summary_table_id}"
    detail_key = f"CHART-detail-{detail_table_id}"
    bar_key = f"CHART-bar-{bar_id}"
    row0_key = "ROW-header-row"
    row1_key = "ROW-bignumber-treemap-row"
    row2_key = "ROW-summary-row"
    row3_key = "ROW-detail-row"
    row4_key = "ROW-bar-row"

    return {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [row0_key, row1_key, row2_key, row3_key, row4_key],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASHBOARD_TITLE}},
        # Fila 0: Titulo dinamico
        row0_key: {
            "type": "ROW",
            "id": row0_key,
            "children": [header_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        header_key: {
            "type": "CHART",
            "id": header_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row0_key],
            "meta": {
                "width": 12,
                "height": 8,
                "chartId": header_id,
                "sliceName": HEADER_CHART_NAME,
            },
        },
        # Fila 1: Big Number + Treemap lado a lado
        row1_key: {
            "type": "ROW",
            "id": row1_key,
            "children": [big_number_key, treemap_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        big_number_key: {
            "type": "CHART",
            "id": big_number_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row1_key],
            "meta": {
                "width": 3,
                "height": 30,
                "chartId": big_number_id,
                "sliceName": BIG_NUMBER_CHART_NAME,
            },
        },
        treemap_key: {
            "type": "CHART",
            "id": treemap_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row1_key],
            "meta": {
                "width": 9,
                "height": 30,
                "chartId": treemap_id,
                "sliceName": TREEMAP_CHART_NAME,
            },
        },
        # Fila 2: Resumen sectorial
        row2_key: {
            "type": "ROW",
            "id": row2_key,
            "children": [summary_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        summary_key: {
            "type": "CHART",
            "id": summary_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row2_key],
            "meta": {
                "width": 12,
                "height": 40,
                "chartId": summary_table_id,
                "sliceName": SUMMARY_TABLE_CHART_NAME,
            },
        },
        # Fila 3: Detalle SCIAN
        row3_key: {
            "type": "ROW",
            "id": row3_key,
            "children": [detail_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        detail_key: {
            "type": "CHART",
            "id": detail_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row3_key],
            "meta": {
                "width": 12,
                "height": 50,
                "chartId": detail_table_id,
                "sliceName": DETAIL_TABLE_CHART_NAME,
            },
        },
        # Fila 4: Barras de municipios
        row4_key: {
            "type": "ROW",
            "id": row4_key,
            "children": [bar_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        bar_key: {
            "type": "CHART",
            "id": bar_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row4_key],
            "meta": {
                "width": 12,
                "height": 50,
                "chartId": bar_id,
                "sliceName": BAR_CHART_NAME,
            },
        },
    }


def create_dashboard(
    client: SupersetClient,
    switchboard_ds_id: int,
    header_id: int,
    big_number_id: int,
    treemap_id: int,
    summary_table_id: int,
    detail_table_id: int,
    bar_id: int,
) -> int:
    position = _build_position_json(header_id, big_number_id, treemap_id, summary_table_id, detail_table_id, bar_id)
    native_filters = _build_native_filters(
        switchboard_ds_id,
        excluir_nivel_scian=[treemap_id, summary_table_id],
    )

    json_metadata: dict[str, Any] = {
        "native_filter_configuration": native_filters,
        "chart_configuration": {},
        "color_scheme": "",
        "label_colors": {},
        "shared_label_colors": {},
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "refresh_frequency": 0,
        "default_filters": "{}",
        "cross_filters_enabled": True,
    }

    existing = find_existing(client, "/api/v1/dashboard/", "slug", DASHBOARD_SLUG)
    if existing:
        dash_id = existing
        print(f"    Dashboard '{DASHBOARD_SLUG}' ya existe (id={dash_id}). Actualizando...")
    else:
        resp = client.post(
            "/api/v1/dashboard/",
            json={
                "dashboard_title": DASHBOARD_TITLE,
                "slug": DASHBOARD_SLUG,
                "published": True,
                "json_metadata": json.dumps(json_metadata),
            },
        )
        dash_id = resp.json()["id"]
        print(f"    Dashboard '{DASHBOARD_TITLE}' creado (id={dash_id}).")

    # PUT con positions dentro de json_metadata dispara set_dash_metadata(),
    # que parsea chartIds de positions y puebla dashboard_slices
    # (la relacion M2M chart <-> dashboard). El endpoint create NO llama
    # a set_dash_metadata — solo update lo hace.
    json_metadata["positions"] = position
    client.put(
        f"/api/v1/dashboard/{dash_id}",
        json={"json_metadata": json.dumps(json_metadata)},
    )
    print("    Metadata del dashboard guardada (filtros + posiciones).")
    return dash_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("==> Esperando a que la API de Superset este lista...")
    _esperar_api(BASE_URL)

    username = os.environ.get("ADMIN_USERNAME", "admin")
    password = os.environ.get("ADMIN_PASSWORD", "admin")

    print("==> Conectando a la API de Superset...")
    client = SupersetClient(BASE_URL, username, password)
    print("    Autenticacion exitosa.")

    try:
        print("\n==> Buscando base de datos...")
        db_id = find_database_id(client)
        print(f"    Usando base de datos 'SIEEJ' (id={db_id}).")

        print("\n==> Provisionando datasets...")
        switchboard_ds_id = create_virtual_dataset(client, db_id)

        print("\n==> Provisionando charts...")
        header_id = create_header_chart(client, switchboard_ds_id)
        big_number_id = create_big_number_chart(client, switchboard_ds_id)
        treemap_id = create_treemap_chart(client, switchboard_ds_id)
        summary_table_id = create_summary_table_chart(client, switchboard_ds_id)
        detail_table_id = create_detail_table_chart(client, switchboard_ds_id)
        bar_id = create_bar_chart(client, switchboard_ds_id)

        print("\n==> Provisionando dashboard...")
        dash_id = create_dashboard(
            client,
            switchboard_ds_id,
            header_id,
            big_number_id,
            treemap_id,
            summary_table_id,
            detail_table_id,
            bar_id,
        )

        print(f"\n==> Listo! Dashboard: http://localhost:8088/superset/dashboard/{DASHBOARD_SLUG}/")
        print(
            f"    IDs: database={db_id},"
            f" switchboard_ds={switchboard_ds_id}, header={header_id},"
            f" big_number={big_number_id}, treemap={treemap_id},"
            f" summary_table={summary_table_id}, detail_table={detail_table_id},"
            f" bar={bar_id}, dashboard={dash_id}"
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
