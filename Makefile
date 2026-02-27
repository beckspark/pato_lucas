.PHONY: docs superset superset-down

# Servir documentacion dbt en http://localhost:8081
docs:
	cd dbt && uv run dbt docs generate && uv run dbt docs serve --port 8081

# Arrancar Superset en http://localhost:8089
superset:
	docker compose up -d

# Detener Superset
superset-down:
	docker compose down
