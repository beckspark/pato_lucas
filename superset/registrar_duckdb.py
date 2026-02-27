"""Registra la base de datos DuckDB en Superset si no existe."""

import json

from superset.app import create_app

app = create_app()
with app.app_context():
    # Imports lazy: superset.models.core requiere app context activo a nivel de modulo
    from superset.extensions import db
    from superset.models.core import Database

    existente = db.session.query(Database).filter_by(database_name="SIEEJ").first()
    if not existente:
        # read_only se pasa via engine_params/connect_args porque duckdb-engine
        # no lo acepta como query parameter en la URI
        extra = json.dumps({"engine_params": {"connect_args": {"read_only": True}}})
        base = Database(
            database_name="SIEEJ",
            sqlalchemy_uri="duckdb:////data/sieej.duckdb",
            expose_in_sqllab=True,
            extra=extra,
        )
        db.session.add(base)
        db.session.commit()
        print("Base de datos SIEEJ registrada.")
    else:
        print("Base de datos SIEEJ ya existe, omitiendo.")
