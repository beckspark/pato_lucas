All comments / natural language throughout the project MUST be in SPANISH
# Database naming

| Element | Rule |
|---------|------|
| Tables | Lowercase, Spanish, plural, no accents, ñ→ni, underscores |
| Columns | Spanish (`anio` not `year`, `estado` not `status`), no accents |
| Prefixes | Pipeline namespace (`ce_`), NO type prefixes (`cat_`, `stg_`) |
| PK / FK | `id` / `singular_table_id` |
| Constraints | `uq_table_columns`, `idx_table_column`, `fk_table_column` |
| Content | Accents allowed in data, proper nouns use `title()` |
