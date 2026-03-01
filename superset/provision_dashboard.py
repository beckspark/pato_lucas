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

import uuid
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
# Constantes CE
# ---------------------------------------------------------------------------

DATASET_VIRTUAL_NAME = "ce_switchboard"
DASHBOARD_SLUG = "censos-economicos"
DASHBOARD_TITLE = "Censos Economicos"
FILTER_NAMESPACE = "censos-economicos"

# Nombres de charts
HEADER_CHART_NAME = "CE -- Titulo dinamico"
BIG_NUMBER_CHART_NAME = "CE -- Total"
TREEMAP_CHART_NAME = "CE -- Sectores economicos"
SUMMARY_TABLE_CHART_NAME = "CE -- Resumen sectorial"
DETAIL_TABLE_CHART_NAME = "CE -- Detalle SCIAN"
BAR_CHART_NAME = "CE -- Municipios por indicador"

# SQL del virtual dataset ce_switchboard.
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
    return metric_simple("valor", "SUM", "SUM(valor)")


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
    return upsert_chart(client, HEADER_CHART_NAME, dataset_id, "handlebars", params)


def create_big_number_chart(client: SupersetClient, dataset_id: int) -> int:
    params = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "big_number_total",
        "metric": _metric_sum_valor(),
        "y_axis_format": "SMART_NUMBER",
        "subtitle": "valor total",
        "adhoc_filters": [],
    }
    return upsert_chart(client, BIG_NUMBER_CHART_NAME, dataset_id, "big_number_total", params)


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
    return upsert_chart(client, TREEMAP_CHART_NAME, dataset_id, "treemap_v2", params)


def create_summary_table_chart(client: SupersetClient, dataset_id: int) -> int:
    """Tabla de resumen sectorial — siempre a nivel sector."""
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
    return upsert_chart(client, SUMMARY_TABLE_CHART_NAME, dataset_id, "table", params)


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
    return upsert_chart(client, DETAIL_TABLE_CHART_NAME, dataset_id, "table", params)


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
    return upsert_chart(client, BAR_CHART_NAME, dataset_id, viz, params)


# ---------------------------------------------------------------------------
# Filtros nativos
# ---------------------------------------------------------------------------


def _build_native_filters(
    ds_id: int,
    *,
    excluir_nivel_scian: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Construye la configuracion de los 7 filtros nativos (sin estrato)."""

    def fid(name: str) -> str:
        return stable_filter_id(FILTER_NAMESPACE, name)

    f_anio_id = fid("anio")
    f_nivel_geo_id = fid("nivel_geografico")
    f_estado_id = fid("estado")
    f_indicador_id = fid("indicador")
    f_nivel_scian_id = fid("nivel_scian")
    f_mun_id = fid("municipio")
    f_sector_id = fid("sector")

    return [
        build_filter(f_anio_id, "Anio", "anio", ds_id, multi=True, default_values=[2024]),
        build_filter(f_nivel_geo_id, "Nivel Geografico", "nivel_geografico", ds_id, default_values=["municipio"]),
        build_filter(
            f_estado_id, "Estado", "nombre_entidad", ds_id, cascade_from=f_nivel_geo_id, default_values=["Jalisco"]
        ),
        build_filter(
            f_indicador_id,
            "Indicador",
            "descripcion_corta",
            ds_id,
            multi=True,
            default_values=["Clave de la unidad económica"],
        ),
        build_filter(
            f_nivel_scian_id,
            "Nivel SCIAN",
            "nivel_scian",
            ds_id,
            default_values=["1. Sector"],
            scope_excluded=excluir_nivel_scian or [],
        ),
        build_filter(f_mun_id, "Municipio", "nombre_municipio", ds_id, multi=True, cascade_from=f_estado_id),
        build_filter(f_sector_id, "Sector", "descripcion_sector", ds_id, multi=True),
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    client, db_id = conectar()

    try:
        print("\n==> Provisionando datasets (CE)...")
        switchboard_ds_id = create_virtual_dataset(client, db_id)

        print("\n==> Provisionando charts (CE)...")
        header_id = create_header_chart(client, switchboard_ds_id)
        big_number_id = create_big_number_chart(client, switchboard_ds_id)
        treemap_id = create_treemap_chart(client, switchboard_ds_id)
        summary_table_id = create_summary_table_chart(client, switchboard_ds_id)
        detail_table_id = create_detail_table_chart(client, switchboard_ds_id)
        bar_id = create_bar_chart(client, switchboard_ds_id)

        print("\n==> Provisionando dashboard (CE)...")
        position = build_position_json(
            DASHBOARD_TITLE,
            [
                # Fila 0: Titulo dinamico
                [("header", header_id, 12, 8, HEADER_CHART_NAME)],
                # Fila 1: Big Number + Treemap
                [
                    ("bignumber", big_number_id, 3, 30, BIG_NUMBER_CHART_NAME),
                    ("treemap", treemap_id, 9, 30, TREEMAP_CHART_NAME),
                ],
                # Fila 2: Resumen sectorial
                [("summary", summary_table_id, 12, 40, SUMMARY_TABLE_CHART_NAME)],
                # Fila 3: Detalle SCIAN
                [("detail", detail_table_id, 12, 50, DETAIL_TABLE_CHART_NAME)],
                # Fila 4: Barras de municipios
                [("bar", bar_id, 12, 50, BAR_CHART_NAME)],
            ],
        )
        native_filters = _build_native_filters(
            switchboard_ds_id,
            excluir_nivel_scian=[treemap_id, summary_table_id],
        )
        dash_id = upsert_dashboard(client, DASHBOARD_SLUG, DASHBOARD_TITLE, position, native_filters)

        print(f"\n==> Listo! Dashboard CE: http://localhost:8088/superset/dashboard/{DASHBOARD_SLUG}/")
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
