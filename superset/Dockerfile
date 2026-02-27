FROM apache/superset:6.0.0

USER root
RUN uv pip install duckdb-engine
USER superset

COPY superset_config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

COPY registrar_duckdb.py /app/registrar_duckdb.py
COPY provision_dashboard.py /app/provision_dashboard.py
COPY superset-init.sh /app/superset-init.sh

ENTRYPOINT ["/bin/bash", "/app/superset-init.sh"]
