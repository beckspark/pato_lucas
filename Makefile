.PHONY: up down docs

# Deteccion de puertos libres; sobreescribir con variables de entorno si se prefiere
# Ejemplo: SUPERSET_PORT=9090 make up
SUPERSET_PORT ?= $(shell python3 scripts/find_free_port.py 8089 2>/dev/null || echo 8089)
DBT_DOCS_PORT ?= $(shell python3 scripts/find_free_port.py 8081 2>/dev/null || echo 8081)

up:
	@echo "==> dbt build..."
	cd dbt && uv run dbt build
	$(MAKE) docs
	@echo "==> Arrancando Superset en puerto $(SUPERSET_PORT)..."
	SUPERSET_PORT=$(SUPERSET_PORT) docker compose up -d --build
	@echo "==> Esperando Superset..."
	@timeout 120 bash -c 'while ! curl -s http://localhost:$(SUPERSET_PORT)/health > /dev/null 2>&1; do sleep 2; done'
	@echo ""
	@echo "=========================================="
	@echo "  Superset (CE):   http://localhost:$(SUPERSET_PORT)/superset/dashboard/censos-economicos/"
	@echo "  Superset (DOF):  http://localhost:$(SUPERSET_PORT)/superset/dashboard/asignaciones-federales/"
	@echo "  dbt docs:        http://localhost:$(DBT_DOCS_PORT)"
	@echo "=========================================="

docs:
	@echo "==> dbt docs generate..."
	cd dbt && uv run dbt docs generate
	@echo "==> Arrancando dbt docs en puerto $(DBT_DOCS_PORT)..."
	cd dbt && nohup uv run dbt docs serve --port $(DBT_DOCS_PORT) > /dev/null 2>&1 &
	@echo "=========================================="
	@echo "  dbt docs:        http://localhost:$(DBT_DOCS_PORT)"
	@echo "=========================================="

down:
	docker compose down
	-pkill -f "dbt docs serve" 2>/dev/null || true
