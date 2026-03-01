#!/usr/bin/env python3
"""Provisiona el dashboard de Asignaciones Federales (DOF) via la API REST de Superset.

Idempotente — seguro de ejecutar multiples veces.
Usa un dataset fisico (fct_dof_asignaciones_mensuales) denormalizado en dbt.

Disenado para ejecutarse como proceso background dentro del contenedor,
esperando a que la API este lista antes de provisionar.
"""

from __future__ import annotations

from typing import Any

from superset_lib import (
    DATASET_SCHEMA,
    SupersetClient,
    build_filter,
    build_position_json,
    conectar,
    find_existing,
    metric_simple,
    stable_filter_id,
    upsert_chart,
    upsert_dashboard,
)

# ---------------------------------------------------------------------------
# Constantes DOF
# ---------------------------------------------------------------------------

DASHBOARD_SLUG = "asignaciones-federales"
DASHBOARD_TITLE = "Asignaciones Federales (DOF)"
DATASET_TABLE_NAME = "fct_dof_asignaciones_mensuales"
FILTER_NAMESPACE = "asignaciones-federales"

# Nombres de charts
HEADER_CHART_NAME = "DOF -- Titulo dinamico"
TIPO_RAMO_CHART_NAME = "DOF -- Total por tipo de ramo"
LINE_CHART_NAME = "DOF -- Serie mensual"
BAR_CHART_NAME = "DOF -- Por estado"
DETAIL_TABLE_CHART_NAME = "DOF -- Detalle"


# ---------------------------------------------------------------------------
# Dataset fisico
# ---------------------------------------------------------------------------


def create_physical_dataset(client: SupersetClient, db_id: int) -> int:
    """Registra fct_dof_asignaciones_mensuales como dataset fisico."""
    existing = find_existing(client, "/api/v1/dataset/", "table_name", DATASET_TABLE_NAME)
    if existing:
        print(f"    Dataset '{DATASET_TABLE_NAME}' ya existe (id={existing}).")
        return existing

    resp = client.post(
        "/api/v1/dataset/",
        json={
            "database": db_id,
            "table_name": DATASET_TABLE_NAME,
            "schema": DATASET_SCHEMA,
        },
    )
    ds_id: int = resp.json()["id"]
    print(f"    Dataset '{DATASET_TABLE_NAME}' creado (id={ds_id}).")
    return ds_id


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def _metric_sum_monto() -> dict[str, Any]:
    return metric_simple("monto", "SUM", "SUM(monto)")


def create_header_chart(client: SupersetClient, dataset_id: int) -> int:
    template = (
        '<div style="padding:4px 0">'
        '<h2 style="margin:0">Asignaciones Federales (DOF)</h2>'
        '<h3 style="margin:0;color:#aaa">'
        "{{#each data}}{{#unless @first}}, {{/unless}}{{anio}}{{/each}}"
        "</h3>"
        "</div>"
    )
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "handlebars",
        "query_mode": "aggregate",
        "groupby": ["anio"],
        "metrics": [],
        "row_limit": 50,
        "order_desc": True,
        "handlebarsTemplate": template,
        "styleTemplate": "",
        "adhoc_filters": [],
    }
    return upsert_chart(client, HEADER_CHART_NAME, dataset_id, "handlebars", params)


def create_tipo_ramo_chart(client: SupersetClient, dataset_id: int) -> int:
    """Bar chart horizontal: SUM(monto) por tipo_ramo, coloreado por anio."""
    viz = "echarts_timeseries_bar"
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz,
        "x_axis": "tipo_ramo",
        "time_grain_sqla": "P1D",
        "metrics": [_metric_sum_monto()],
        "groupby": ["anio"],
        "order_desc": True,
        "row_limit": 50,
        "color_scheme": "supersetColors",
        "show_legend": True,
        "truncate_metric": True,
        "show_empty_columns": False,
        "y_axis_format": "$,.0f",
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "orientation": "horizontal",
        "adhoc_filters": [],
    }
    return upsert_chart(client, TIPO_RAMO_CHART_NAME, dataset_id, viz, params)


def create_line_chart(client: SupersetClient, dataset_id: int) -> int:
    viz = "echarts_timeseries_line"
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz,
        "x_axis": "mes_numero",
        "time_grain_sqla": "P1D",
        "metrics": [_metric_sum_monto()],
        "groupby": ["anio", "tipo_ramo"],
        "order_desc": False,
        "row_limit": 200,
        "color_scheme": "supersetColors",
        "show_legend": True,
        "rich_tooltip": True,
        "y_axis_format": "$,.0f",
        "tooltipTimeFormat": "smart_date",
        "truncate_metric": True,
        "show_empty_columns": False,
        "adhoc_filters": [],
    }
    return upsert_chart(client, LINE_CHART_NAME, dataset_id, viz, params)


def create_bar_chart(client: SupersetClient, dataset_id: int) -> int:
    viz = "echarts_timeseries_bar"
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz,
        "x_axis": "nombre_entidad",
        "time_grain_sqla": "P1D",
        "metrics": [_metric_sum_monto()],
        "groupby": ["tipo_ramo"],
        "order_desc": True,
        "row_limit": 50,
        "color_scheme": "supersetColors",
        "show_legend": True,
        "x_axis_sort_asc": False,
        "x_axis_sort_series": "sum",
        "x_axis_sort_series_ascending": False,
        "truncate_metric": True,
        "show_empty_columns": False,
        "y_axis_format": "$,.0f",
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "orientation": "vertical",
        "adhoc_filters": [],
    }
    return upsert_chart(client, BAR_CHART_NAME, dataset_id, viz, params)


def create_detail_table_chart(client: SupersetClient, dataset_id: int) -> int:
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "query_mode": "aggregate",
        "groupby": ["nombre_entidad", "anio", "tipo_ramo", "fondo", "mes_numero", "mes_nombre"],
        "metrics": [_metric_sum_monto()],
        "row_limit": 1000,
        "server_pagination": True,
        "server_page_length": 100,
        "order_desc": False,
        "orderby": [["anio", True], ["mes_numero", True]],
        "include_search": True,
        "adhoc_filters": [],
    }
    return upsert_chart(client, DETAIL_TABLE_CHART_NAME, dataset_id, "table", params)


# ---------------------------------------------------------------------------
# Filtros nativos
# ---------------------------------------------------------------------------


def _build_native_filters(ds_id: int) -> list[dict[str, Any]]:
    """Construye los 5 filtros nativos del dashboard DOF."""

    def fid(name: str) -> str:
        return stable_filter_id(FILTER_NAMESPACE, name)

    f_anio_id = fid("anio")
    f_estado_id = fid("estado")
    f_tipo_ramo_id = fid("tipo_ramo")
    f_fondo_id = fid("fondo")
    f_mes_id = fid("mes")

    return [
        # 1. Anio
        build_filter(f_anio_id, "Anio", "anio", ds_id, multi=True, search_all=False, default_values=[2025]),
        # 2. Estado (multi-select, vacio = vista nacional)
        build_filter(
            f_estado_id,
            "Estado",
            "nombre_entidad",
            ds_id,
            multi=True,
            enable_empty=True,
            search_all=False,
            default_values=["Jalisco"],
        ),
        # 3. Tipo de ramo
        build_filter(f_tipo_ramo_id, "Tipo de ramo", "tipo_ramo", ds_id, multi=True, search_all=False),
        # 4. Fondo (cascada desde Tipo de ramo)
        build_filter(f_fondo_id, "Fondo", "fondo", ds_id, multi=True, search_all=False, cascade_from=f_tipo_ramo_id),
        # 5. Mes
        build_filter(f_mes_id, "Mes", "mes_nombre", ds_id, multi=True, search_all=False),
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    client, db_id = conectar()

    try:
        print("\n==> Provisionando dataset (DOF)...")
        ds_id = create_physical_dataset(client, db_id)

        print("\n==> Provisionando charts (DOF)...")
        header_id = create_header_chart(client, ds_id)
        tipo_ramo_id = create_tipo_ramo_chart(client, ds_id)
        line_id = create_line_chart(client, ds_id)
        bar_id = create_bar_chart(client, ds_id)
        detail_id = create_detail_table_chart(client, ds_id)

        print("\n==> Provisionando dashboard (DOF)...")
        position = build_position_json(
            DASHBOARD_TITLE,
            [
                # Fila 0: Titulo dinamico (12 col, h=8)
                [("header", header_id, 12, 8, HEADER_CHART_NAME)],
                # Fila 1: Total por tipo (4 col) + Serie mensual (8 col), h=30
                [
                    ("tipo_ramo", tipo_ramo_id, 4, 30, TIPO_RAMO_CHART_NAME),
                    ("line", line_id, 8, 30, LINE_CHART_NAME),
                ],
                # Fila 2: Por estado (12 col, h=50)
                [("bar", bar_id, 12, 50, BAR_CHART_NAME)],
                # Fila 3: Detalle (12 col, h=50)
                [("detail", detail_id, 12, 50, DETAIL_TABLE_CHART_NAME)],
            ],
        )
        native_filters = _build_native_filters(ds_id)
        dash_id = upsert_dashboard(client, DASHBOARD_SLUG, DASHBOARD_TITLE, position, native_filters)

        print(f"\n==> Listo! Dashboard DOF: http://localhost:8088/superset/dashboard/{DASHBOARD_SLUG}/")
        print(
            f"    IDs: database={db_id},"
            f" dataset={ds_id}, header={header_id},"
            f" tipo_ramo={tipo_ramo_id}, line={line_id},"
            f" bar={bar_id}, detail={detail_id},"
            f" dashboard={dash_id}"
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
