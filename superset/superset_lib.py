"""Libreria compartida para provisionar dashboards en Superset.

Contiene el cliente HTTP, helpers de espera, busqueda, upsert de charts,
construccion de filtros nativos y layout de posiciones.
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

BASE_URL = "http://localhost:8088"
MAX_INTENTOS = 30
INTERVALO_RETRY = 2  # segundos
DATASET_SCHEMA = "sieej.mart"


# ---------------------------------------------------------------------------
# Cliente HTTP (usa requests, ya incluido en la imagen de Superset)
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


def esperar_api(base_url: str = BASE_URL) -> None:
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
# Helpers de busqueda
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
# Upsert de charts
# ---------------------------------------------------------------------------


def upsert_chart(
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


# ---------------------------------------------------------------------------
# Helper de metricas
# ---------------------------------------------------------------------------


def metric_simple(column: str, aggregate: str, label: str) -> dict[str, Any]:
    """Metrica reutilizable: AGGREGATE(columna)."""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column, "type": "FLOAT"},
        "aggregate": aggregate,
        "label": label,
    }


# ---------------------------------------------------------------------------
# Filtros nativos
# ---------------------------------------------------------------------------


def stable_filter_id(namespace: str, name: str) -> str:
    """ID de filtro deterministico a partir del namespace + nombre."""
    raw = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{namespace}.{name}")).upper()
    return f"NATIVE_FILTER-{raw}"


def build_filter(
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
    """Construye la configuracion de un filtro nativo de Superset."""
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


# ---------------------------------------------------------------------------
# Layout de posiciones
# ---------------------------------------------------------------------------


def build_position_json(
    dashboard_title: str,
    rows: list[list[tuple[str, int, int, int, str]]],
) -> dict[str, Any]:
    """Construye el JSON de posiciones del layout del dashboard.

    rows: lista de filas. Cada fila es una lista de tuplas:
        (chart_key_suffix, chart_id, width, height, slice_name)
    """
    row_keys = []
    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": dashboard_title}},
    }

    for row_idx, row_charts in enumerate(rows):
        row_key = f"ROW-row-{row_idx}"
        row_keys.append(row_key)
        chart_keys_in_row = []

        for suffix, chart_id, width, height, slice_name in row_charts:
            chart_key = f"CHART-{suffix}-{chart_id}"
            chart_keys_in_row.append(chart_key)
            position[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_key],
                "meta": {
                    "width": width,
                    "height": height,
                    "chartId": chart_id,
                    "sliceName": slice_name,
                },
            }

        position[row_key] = {
            "type": "ROW",
            "id": row_key,
            "children": chart_keys_in_row,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

    position["ROOT_ID"] = {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]}
    position["GRID_ID"] = {
        "type": "GRID",
        "id": "GRID_ID",
        "children": row_keys,
        "parents": ["ROOT_ID"],
    }

    return position


# ---------------------------------------------------------------------------
# Dashboard upsert
# ---------------------------------------------------------------------------


def upsert_dashboard(
    client: SupersetClient,
    slug: str,
    title: str,
    position: dict[str, Any],
    native_filters: list[dict[str, Any]],
) -> int:
    """Crea o actualiza un dashboard con su layout y filtros."""
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

    existing = find_existing(client, "/api/v1/dashboard/", "slug", slug)
    if existing:
        dash_id = existing
        print(f"    Dashboard '{slug}' ya existe (id={dash_id}). Actualizando...")
    else:
        resp = client.post(
            "/api/v1/dashboard/",
            json={
                "dashboard_title": title,
                "slug": slug,
                "published": True,
                "json_metadata": json.dumps(json_metadata),
            },
        )
        dash_id = resp.json()["id"]
        print(f"    Dashboard '{title}' creado (id={dash_id}).")

    # PUT con positions dentro de json_metadata dispara set_dash_metadata(),
    # que parsea chartIds de positions y puebla dashboard_slices
    json_metadata["positions"] = position
    client.put(
        f"/api/v1/dashboard/{dash_id}",
        json={"json_metadata": json.dumps(json_metadata)},
    )
    print("    Metadata del dashboard guardada (filtros + posiciones).")
    return dash_id


# ---------------------------------------------------------------------------
# Conexion compartida
# ---------------------------------------------------------------------------


def conectar() -> tuple[SupersetClient, int]:
    """Espera API, autentica y retorna (client, database_id)."""
    print("==> Esperando a que la API de Superset este lista...")
    esperar_api(BASE_URL)

    username = os.environ.get("ADMIN_USERNAME", "admin")
    password = os.environ.get("ADMIN_PASSWORD", "admin")

    print("==> Conectando a la API de Superset...")
    client = SupersetClient(BASE_URL, username, password)
    print("    Autenticacion exitosa.")

    print("\n==> Buscando base de datos...")
    db_id = find_database_id(client)
    print(f"    Usando base de datos 'SIEEJ' (id={db_id}).")

    return client, db_id
