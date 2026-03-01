FROM apache/superset:6.0.0

USER root
RUN uv pip install duckdb-engine
USER superset

COPY superset_config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

COPY registrar_duckdb.py /app/registrar_duckdb.py
COPY superset_lib.py /app/superset_lib.py
COPY provision_dashboard.py /app/provision_dashboard.py
COPY provision_dof_dashboard.py /app/provision_dof_dashboard.py
COPY superset-init.sh /app/superset-init.sh

ENTRYPOINT ["/bin/bash", "/app/superset-init.sh"]
