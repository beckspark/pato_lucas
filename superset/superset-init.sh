#!/bin/bash
set -e

# Migrar esquema de metadata
superset db upgrade

# Crear usuario admin (idempotente — solo si no existe)
if ! superset fab list-users | grep -q "${ADMIN_USERNAME:-admin}"; then
    superset fab create-admin \
        --username "${ADMIN_USERNAME:-admin}" \
        --firstname Admin \
        --lastname Superset \
        --email admin@localhost \
        --password "${ADMIN_PASSWORD:-admin}"
fi

# Inicializar roles y permisos
superset init

# Registrar conexion DuckDB (idempotente)
python /app/registrar_duckdb.py

# Arrancar servidor
exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 2 \
    --timeout 120 \
    "superset.app:create_app()"
