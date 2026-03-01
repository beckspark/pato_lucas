PORT ?= 8081

.PHONY: docs superset superset-down

# Servir documentacion dbt en http://localhost:$(PORT)
docs:
	cd dbt && uv run dbt docs generate && uv run dbt docs serve --port $(PORT)

# Arrancar Superset en http://localhost:8089
superset:
	docker compose up -d

# Detener Superset
superset-down:
	docker compose down
