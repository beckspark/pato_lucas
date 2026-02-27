#!/usr/bin/env python3
"""Provisiona el dashboard de Censos Economicos via la API REST de Superset.

Idempotente — seguro de ejecutar multiples veces. Verifica recursos existentes antes de crear.

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

DATASET_NAME = "fct_censo_economico"
DATASET_SCHEMA = "sieej.mart"
DASHBOARD_SLUG = "censos-economicos"
DASHBOARD_TITLE = "Censos Económicos"

BAR_CHART_NAME = "CE — Municipios por indicador"
TABLE_CHART_NAME = "CE — Datos crudos"
HEADER_CHART_NAME = "CE — Título dinámico"
BIG_NUMBER_CHART_NAME = "CE — Total"
TREEMAP_CHART_NAME = "CE — Sectores económicos"

ALL_COLUMNS = [
    "anio",
    "nombre_entidad",
    "nombre_municipio",
    "indicador",
    "descripcion",
    "descripcion_actividad",
    "clasificador_actividad",
    "descripcion_estrato",
    "unidad",
    "valor",
]

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
# Dataset (tabla fisica — sin SQL virtual)
# ---------------------------------------------------------------------------


def create_dataset(client: SupersetClient, db_id: int) -> int:
    existing = find_existing(client, "/api/v1/dataset/", "table_name", DATASET_NAME)
    if existing:
        print(f"    Dataset '{DATASET_NAME}' ya existe (id={existing}). Omitiendo.")
        return existing

    resp = client.post(
        "/api/v1/dataset/",
        json={
            "database": db_id,
            "table_name": DATASET_NAME,
            "schema": DATASET_SCHEMA,
        },
    )
    ds_id: int = resp.json()["id"]
    print(f"    Dataset '{DATASET_NAME}' creado (id={ds_id}).")
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


def create_header_chart(client: SupersetClient, dataset_id: int) -> int:
    existing = find_existing(client, "/api/v1/chart/", "slice_name", HEADER_CHART_NAME)
    params = _header_params(dataset_id)
    payload = {
        "slice_name": HEADER_CHART_NAME,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "viz_type": "handlebars",
        "params": json.dumps(params),
    }
    if existing:
        print(f"    Chart '{HEADER_CHART_NAME}' ya existe (id={existing}). Actualizando...")
        client.put(f"/api/v1/chart/{existing}", json=payload)
        return existing

    resp = client.post("/api/v1/chart/", json=payload)
    chart_id: int = resp.json()["id"]
    print(f"    Chart '{HEADER_CHART_NAME}' creado (id={chart_id}).")
    return chart_id


def _header_params(dataset_id: int) -> dict[str, Any]:
    # Handlebars consulta el dataset (los filtros nativos aplican) y renderiza
    # el resultado con un template — a diferencia de markdown que es estatico.
    template = (
        '<div style="padding:4px 0">'
        '<h2 style="margin:0">Censos Económicos</h2>'
        '<h3 style="margin:0;color:#aaa">'
        "{{#with (lookup data 0)}}{{nombre_entidad}}{{/with}}"
        " · "
        "{{#each data}}{{#unless @first}}, {{/unless}}{{descripcion}}{{/each}}"
        "</h3>"
        "</div>"
    )
    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": "handlebars",
        "query_mode": "aggregate",
        "groupby": ["nombre_entidad", "descripcion"],
        "metrics": [],
        "row_limit": 50,
        "order_desc": True,
        "handlebarsTemplate": template,
        "styleTemplate": "",
        "adhoc_filters": [],
    }


def create_big_number_chart(client: SupersetClient, dataset_id: int) -> int:
    existing = find_existing(client, "/api/v1/chart/", "slice_name", BIG_NUMBER_CHART_NAME)
    params = _big_number_params(dataset_id)
    if existing:
        print(f"    Chart '{BIG_NUMBER_CHART_NAME}' ya existe (id={existing}). Actualizando...")
        client.put(f"/api/v1/chart/{existing}", json={"params": json.dumps(params)})
        return existing

    resp = client.post(
        "/api/v1/chart/",
        json={
            "slice_name": BIG_NUMBER_CHART_NAME,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": "big_number_total",
            "params": json.dumps(params),
        },
    )
    chart_id: int = resp.json()["id"]
    print(f"    Chart '{BIG_NUMBER_CHART_NAME}' creado (id={chart_id}).")
    return chart_id


def _big_number_params(dataset_id: int) -> dict[str, Any]:
    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": "big_number_total",
        "metric": _metric_sum_valor(),
        "y_axis_format": "SMART_NUMBER",
        "subtitle": "valor total",
        "adhoc_filters": [],
    }


def create_treemap_chart(client: SupersetClient, dataset_id: int) -> int:
    existing = find_existing(client, "/api/v1/chart/", "slice_name", TREEMAP_CHART_NAME)
    params = _treemap_params(dataset_id)
    if existing:
        print(f"    Chart '{TREEMAP_CHART_NAME}' ya existe (id={existing}). Actualizando...")
        client.put(f"/api/v1/chart/{existing}", json={"params": json.dumps(params)})
        return existing

    resp = client.post(
        "/api/v1/chart/",
        json={
            "slice_name": TREEMAP_CHART_NAME,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": "treemap_v2",
            "params": json.dumps(params),
        },
    )
    chart_id: int = resp.json()["id"]
    print(f"    Chart '{TREEMAP_CHART_NAME}' creado (id={chart_id}).")
    return chart_id


def _treemap_params(dataset_id: int) -> dict[str, Any]:
    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": "treemap_v2",
        "groupby": ["descripcion_actividad"],
        "metric": _metric_sum_valor(),
        "show_labels": True,
        "show_upper_labels": True,
        "number_format": "SMART_NUMBER",
        "row_limit": 50,
        "color_scheme": "supersetColors",
        "adhoc_filters": [],
    }


def create_bar_chart(client: SupersetClient, dataset_id: int) -> int:
    existing = find_existing(client, "/api/v1/chart/", "slice_name", BAR_CHART_NAME)
    if existing:
        print(f"    Chart '{BAR_CHART_NAME}' ya existe (id={existing}). Omitiendo.")
        return existing

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

    resp = client.post(
        "/api/v1/chart/",
        json={
            "slice_name": BAR_CHART_NAME,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": viz,
            "params": json.dumps(params),
        },
    )
    chart_id: int = resp.json()["id"]
    print(f"    Chart '{BAR_CHART_NAME}' creado (id={chart_id}).")
    return chart_id


def create_table_chart(client: SupersetClient, dataset_id: int) -> int:
    existing = find_existing(client, "/api/v1/chart/", "slice_name", TABLE_CHART_NAME)
    if existing:
        print(f"    Chart '{TABLE_CHART_NAME}' ya existe (id={existing}). Omitiendo.")
        return existing

    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": ALL_COLUMNS,
        "row_limit": 1000,
        "server_pagination": True,
        "server_page_length": 100,
        "include_search": True,
        "order_desc": True,
        "adhoc_filters": [],
    }

    resp = client.post(
        "/api/v1/chart/",
        json={
            "slice_name": TABLE_CHART_NAME,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": "table",
            "params": json.dumps(params),
        },
    )
    chart_id: int = resp.json()["id"]
    print(f"    Chart '{TABLE_CHART_NAME}' creado (id={chart_id}).")
    return chart_id


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


def _stable_filter_id(name: str) -> str:
    """ID de filtro deterministico a partir del nombre — sobrevive re-provisiones."""
    raw = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"censos-economicos.{name}")).upper()
    return f"NATIVE_FILTER-{raw}"


def _build_native_filters(dataset_id: int) -> list[dict[str, Any]]:
    """Construye la configuracion de los 5 filtros nativos."""
    f_anio_id = _stable_filter_id("anio")
    f_estado_id = _stable_filter_id("estado")
    f_indicador_id = _stable_filter_id("indicador")
    f_estrato_id = _stable_filter_id("estrato")
    f_mun_id = _stable_filter_id("municipio")

    def _filter(
        fid: str,
        name: str,
        column: str,
        *,
        multi: bool = False,
        cascade_from: str | list[str] | None = None,
        enable_empty: bool = False,
        search_all: bool = True,
        default_values: list[Any] | None = None,
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
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
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

    # Cascada: Año (top-level), Estado (top-level) → Indicador → Estrato → Municipio
    # Municipio necesita Estado como padre directo porque las cascadas NO son transitivas.
    filters = [
        _filter(f_anio_id, "Año", "anio", multi=True, default_values=[2024]),
        _filter(f_estado_id, "Estado", "nombre_entidad", default_values=["Jalisco"]),
        _filter(
            f_indicador_id,
            "Indicador",
            "descripcion",
            multi=True,
            cascade_from=f_estado_id,
            default_values=["Personal ocupado total"],
        ),
        _filter(
            f_estrato_id,
            "Estrato",
            "descripcion_estrato",
            search_all=False,
            cascade_from=f_indicador_id,
        ),
        _filter(
            f_mun_id,
            "Municipio",
            "nombre_municipio",
            multi=True,
            cascade_from=[f_estado_id, f_estrato_id],
        ),
    ]
    return filters


def _build_position_json(
    header_chart_id: int,
    big_number_chart_id: int,
    treemap_chart_id: int,
    bar_chart_id: int,
    table_chart_id: int,
) -> dict[str, Any]:
    """Construye el JSON de posiciones del layout del dashboard."""
    header_key = f"CHART-header-{header_chart_id}"
    big_number_key = f"CHART-bignumber-{big_number_chart_id}"
    treemap_key = f"CHART-treemap-{treemap_chart_id}"
    bar_key = f"CHART-bar-{bar_chart_id}"
    table_key = f"CHART-table-{table_chart_id}"
    row0_key = "ROW-header-row"
    row1_key = "ROW-bignumber-row"
    row2_key = "ROW-treemap-row"
    row3_key = "ROW-bar-row"
    row4_key = "ROW-table-row"

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
                "chartId": header_chart_id,
                "sliceName": HEADER_CHART_NAME,
            },
        },
        row1_key: {
            "type": "ROW",
            "id": row1_key,
            "children": [big_number_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        big_number_key: {
            "type": "CHART",
            "id": big_number_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row1_key],
            "meta": {
                "width": 12,
                "height": 10,
                "chartId": big_number_chart_id,
                "sliceName": BIG_NUMBER_CHART_NAME,
            },
        },
        row2_key: {
            "type": "ROW",
            "id": row2_key,
            "children": [treemap_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        treemap_key: {
            "type": "CHART",
            "id": treemap_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row2_key],
            "meta": {
                "width": 12,
                "height": 40,
                "chartId": treemap_chart_id,
                "sliceName": TREEMAP_CHART_NAME,
            },
        },
        row3_key: {
            "type": "ROW",
            "id": row3_key,
            "children": [bar_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        bar_key: {
            "type": "CHART",
            "id": bar_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row3_key],
            "meta": {
                "width": 12,
                "height": 50,
                "chartId": bar_chart_id,
                "sliceName": BAR_CHART_NAME,
            },
        },
        row4_key: {
            "type": "ROW",
            "id": row4_key,
            "children": [table_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        table_key: {
            "type": "CHART",
            "id": table_key,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row4_key],
            "meta": {
                "width": 12,
                "height": 60,
                "chartId": table_chart_id,
                "sliceName": TABLE_CHART_NAME,
            },
        },
    }


def create_dashboard(
    client: SupersetClient,
    dataset_id: int,
    header_id: int,
    big_number_id: int,
    treemap_id: int,
    bar_id: int,
    table_id: int,
) -> int:
    position = _build_position_json(header_id, big_number_id, treemap_id, bar_id, table_id)
    native_filters = _build_native_filters(dataset_id)

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
        print(f"    Dashboard '{DASHBOARD_SLUG}' ya existe (id={dash_id}). Actualizando filtros...")
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

        print("\n==> Provisionando dataset...")
        dataset_id = create_dataset(client, db_id)

        print("\n==> Provisionando charts...")
        header_id = create_header_chart(client, dataset_id)
        big_number_id = create_big_number_chart(client, dataset_id)
        treemap_id = create_treemap_chart(client, dataset_id)
        bar_id = create_bar_chart(client, dataset_id)
        table_id = create_table_chart(client, dataset_id)

        print("\n==> Provisionando dashboard...")
        dash_id = create_dashboard(client, dataset_id, header_id, big_number_id, treemap_id, bar_id, table_id)

        print(f"\n==> Listo! Dashboard: http://localhost:8088/superset/dashboard/{DASHBOARD_SLUG}/")
        print(
            f"    IDs: database={db_id}, dataset={dataset_id}, header={header_id},"
            f" big_number={big_number_id}, treemap={treemap_id},"
            f" bar={bar_id}, table={table_id}, dashboard={dash_id}"
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
