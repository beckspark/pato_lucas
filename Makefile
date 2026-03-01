.PHONY: up down

# Deteccion de puertos libres via python (pipe a python3 para preservar newlines)
find_free_port = $(shell echo -e 'import socket\nfor p in range($(1),$(1)+100):\n try:\n  s=socket.socket();s.bind(("",p));s.close();print(p);break\n except OSError:pass' | python3)

SUPERSET_PORT := $(call find_free_port,8089)
DBT_DOCS_PORT := $(call find_free_port,8081)

up:
	@echo "==> dbt build..."
	cd dbt && uv run dbt build
	@echo "==> dbt docs generate..."
	cd dbt && uv run dbt docs generate
	@echo "==> Arrancando Superset en puerto $(SUPERSET_PORT)..."
	SUPERSET_PORT=$(SUPERSET_PORT) docker compose up -d --build
	@echo "==> Arrancando dbt docs en puerto $(DBT_DOCS_PORT)..."
	cd dbt && nohup uv run dbt docs serve --port $(DBT_DOCS_PORT) > /dev/null 2>&1 &
	@echo "==> Esperando Superset..."
	@timeout 120 bash -c 'while ! curl -s http://localhost:$(SUPERSET_PORT)/health > /dev/null 2>&1; do sleep 2; done'
	@echo ""
	@echo "=========================================="
	@echo "  Superset (CE):   http://localhost:$(SUPERSET_PORT)/superset/dashboard/censos-economicos/"
	@echo "  Superset (DOF):  http://localhost:$(SUPERSET_PORT)/superset/dashboard/asignaciones-federales/"
	@echo "  dbt docs:        http://localhost:$(DBT_DOCS_PORT)"
	@echo "=========================================="

down:
	docker compose down
	-pkill -f "dbt docs serve" 2>/dev/null || true
