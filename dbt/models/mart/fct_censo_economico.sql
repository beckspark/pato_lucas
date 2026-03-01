-- Censo Económico pivotado con todos los niveles geográficos y SCIAN.
-- Los 98 indicadores permanecen como columnas para lectura eficiente en DuckDB
-- columnar.
{{
    config(
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_nivel_geo ON {{ this }} (nivel_geografico)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_nivel_scian ON {{ this }} (nivel_scian)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_sector ON {{ this }} (codigo_sector)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_cve_ent ON {{ this }} (cve_ent)",
        ]
    )
}}

select *
from {{ ref("int_ce__datos") }}
