"""Configuracion de Apache Superset para el proyecto Pato Lucas."""

import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "clave-temporal-solo-desarrollo-local")

# Permitir conexiones a bases de datos locales (DuckDB via file://)
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Idioma espaniol por defecto
BABEL_DEFAULT_LOCALE = "es"
LANGUAGES = {
    "es": {"flag": "es", "name": "Español"},
    "en": {"flag": "us", "name": "English"},
}

# Metadata en SQLite (default, persistida en superset_home volume)
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"


# Filtrar schemas DuckDB visibles en SQL Lab
# Controlado via SUPERSET_ALLOWED_SCHEMAS (separados por coma)
_SCHEMAS_PERMITIDOS = os.environ.get("SUPERSET_ALLOWED_SCHEMAS", "")


def _configurar_schemas_duckdb():
    if not _SCHEMAS_PERMITIDOS:
        return

    permitidos = {s.strip() for s in _SCHEMAS_PERMITIDOS.split(",") if s.strip()}

    from superset.db_engine_specs.duckdb import DuckDBEngineSpec

    _original = DuckDBEngineSpec.get_schema_names

    @classmethod  # type: ignore[misc]
    def get_schema_names(cls, inspector, **kwargs):
        todos = _original.__func__(cls, inspector, **kwargs)
        return todos & permitidos

    DuckDBEngineSpec.get_schema_names = get_schema_names


_configurar_schemas_duckdb()
