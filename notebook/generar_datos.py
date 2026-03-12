"""
Genera una base de datos DuckDB sintética para el tutorial de dbt.

Dos fuentes simplificadas del dominio real SIEEJ:
- Censo Económico (CE): ~670 filas de datos + catálogos + diccionario
- Presupuesto DOF: ~120 filas con datos sucios intencionales

Ejecutar:
    uv run python generar_datos.py

Genera: datos_tutorial.duckdb
"""

import random
from pathlib import Path

import duckdb

# ── Semilla para reproducibilidad ──
random.seed(42)

BD_DESTINO = Path(__file__).parent / "datos_tutorial.duckdb"

# ── Catálogos geográficos ──
ENTIDADES = [
    ("09", "Ciudad de México"),
    ("11", "Guanajuato"),
    ("14", "Jalisco"),
    ("19", "Nuevo León"),
    ("21", "Puebla"),
]

MUNICIPIOS = {
    "09": [("003", "Coyoacán"), ("015", "Cuauhtémoc"), ("007", "Iztapalapa")],
    "11": [("020", "León"), ("017", "Irapuato"), ("007", "Celaya")],
    "14": [("039", "Guadalajara"), ("120", "Zapopan"), ("098", "San Pedro Tlaquepaque")],
    "19": [("039", "Monterrey"), ("018", "García"), ("006", "Apodaca")],
    "21": [("114", "Puebla"), ("132", "San Martín Texmelucan"), ("119", "San Andrés Cholula")],
}

# ── Catálogo SCIAN simplificado ──
# (codigo, descripcion, clasificador)
ACTIVIDADES_SCIAN = [
    # Sector compuesto 31-33
    ("31-33", "Industrias manufactureras", "Sector"),
    ("311", "Industria alimentaria", "Subsector"),
    ("3111", "Elaboración de alimentos para animales", "Rama"),
    ("3112", "Molienda de granos y obtención de aceites", "Rama"),
    ("312", "Industria de las bebidas y del tabaco", "Subsector"),
    ("3121", "Industria de las bebidas", "Rama"),
    # Sector 46
    ("46", "Comercio al por menor", "Sector"),
    ("461", "Comercio al por menor de abarrotes y alimentos", "Subsector"),
    ("4611", "Comercio al por menor de abarrotes", "Rama"),
    ("462", "Comercio al por menor en tiendas de autoservicio", "Subsector"),
    ("4621", "Comercio en tiendas de autoservicio y departamentales", "Rama"),
    # Sector 72
    ("72", "Servicios de alojamiento y preparación de alimentos", "Sector"),
    ("722", "Servicios de preparación de alimentos y bebidas", "Subsector"),
    ("7221", "Restaurantes con servicio completo", "Rama"),
    ("7222", "Restaurantes de autoservicio y comida para llevar", "Rama"),
]

# Códigos de actividad que aparecen en ce_datos (todos los del catálogo)
CODIGOS_ACTIVIDAD = [a[0] for a in ACTIVIDADES_SCIAN]

ANIOS = [2019, 2024]

# ── Indicadores del Censo Económico (10 simplificados) ──
INDICADORES_CE = ["ue", "h001a", "h000a", "i000a", "j000a", "k000a", "a111a", "m000a", "p000a", "q000a"]

DICCIONARIO_INDICADORES = {
    "ue": ("Unidades económicas (número)", "numérico"),
    "h001a": "Personal ocupado total (personas)",
    "h000a": "Personal ocupado dependiente de la razón social (personas)",
    "i000a": "Total de remuneraciones (miles de pesos)",
    "j000a": "Producción bruta total (miles de pesos)",
    "k000a": "Consumo intermedio (miles de pesos)",
    "a111a": "Valor agregado censal bruto (miles de pesos)",
    "m000a": "Formación bruta de capital fijo (miles de pesos)",
    "p000a": "Activos fijos netos (miles de pesos)",
    "q000a": "Ingresos por suministro de bienes y servicios (miles de pesos)",
}

# Filas de metadatos que el staging debe filtrar
METADATOS_DICCIONARIO = [
    ("ENTIDAD", "Clave de la entidad federativa", "texto"),
    ("MUNICIPIO", "Clave del municipio", "texto"),
    ("CODIGO", "Código de actividad SCIAN", "texto"),
    ("ID_ESTRATO", "Identificador de estrato", "texto"),
    ("E000", "Clasificador de sector", "texto"),
]

# ── Fondos DOF ──
FONDOS_DOF = [
    (28, 1, "Fondo General de Participaciones"),
    (28, 2, "Fondo de Fomento Municipal"),
    (33, 1, "FAIS"),
    (33, 2, "FORTAMUN"),
    (33, 3, "FAM"),
    (23, 1, "FIEF"),
]

MESES = [
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre",
]

# ── Variantes sucias de nombres de entidad para DOF ──
NOMBRES_SUCIOS: dict[str, list[str]] = {
    "09": ["  Ciudad de México", "CIUDAD DE MEXICO", "* Ciudad de México"],
    "11": ["Guanajuato", " GUANAJUATO ", "* Guanajuato"],
    "14": ["Jalisco", "JALISCO", "  jalisco"],
    "19": ["Nuevo León", "NUEVO LEON", "* Nuevo León "],
    "21": ["Puebla", "PUEBLA", " * Puebla"],
}


def _valor_aleatorio(base: int, variacion: float = 0.3) -> int:
    """Genera un valor aleatorio alrededor de una base con variación porcentual."""
    return max(0, int(base * (1 + random.uniform(-variacion, variacion))))


def _generar_indicadores(escala: float = 1.0) -> dict[str, int | None]:
    """Genera valores para los 10 indicadores con una escala dada."""
    bases = {
        "ue": 150,
        "h001a": 800,
        "h000a": 650,
        "i000a": 45000,
        "j000a": 120000,
        "k000a": 65000,
        "a111a": 55000,
        "m000a": 12000,
        "p000a": 80000,
        "q000a": 110000,
    }
    resultado = {}
    for ind, base in bases.items():
        # Introducir ~5% de NULLs aleatorios
        if random.random() < 0.05:
            resultado[ind] = None
        else:
            resultado[ind] = _valor_aleatorio(int(base * escala))
    return resultado


def crear_ce_catalogos_entidades_municipios(con: duckdb.DuckDBPyConnection) -> None:
    """Crea tabla de catálogo geográfico (~20 filas)."""
    filas = []
    for cve_ent, nombre_ent in ENTIDADES:
        for cve_mun, nombre_mun in MUNICIPIOS[cve_ent]:
            cvegeo = cve_ent + cve_mun
            # Añadir espacios extra intencionales en algunos nombres
            nom_ent = f"  {nombre_ent}" if random.random() < 0.3 else nombre_ent
            nom_mun = f"{nombre_mun}  " if random.random() < 0.3 else nombre_mun
            filas.append((cvegeo, cve_ent, nom_ent, cve_mun, nom_mun))

    con.execute("DROP TABLE IF EXISTS ce_catalogos_entidades_municipios")
    con.execute("""
        CREATE TABLE ce_catalogos_entidades_municipios (
            cvegeo VARCHAR, cve_ent VARCHAR, nombre_entidad VARCHAR,
            cve_mun VARCHAR, nombre_municipio VARCHAR
        )
    """)
    con.executemany("INSERT INTO ce_catalogos_entidades_municipios VALUES (?, ?, ?, ?, ?)", filas)
    print(f"  ce_catalogos_entidades_municipios: {len(filas)} filas")


def crear_ce_catalogos_actividades(con: duckdb.DuckDBPyConnection) -> None:
    """Crea tabla de catálogo SCIAN (~15 filas)."""
    filas = []
    for codigo, desc, clasif in ACTIVIDADES_SCIAN:
        # Espacios extra intencionales
        cod = f" {codigo} " if random.random() < 0.2 else codigo
        filas.append((cod, desc, clasif))

    con.execute("DROP TABLE IF EXISTS ce_catalogos_actividades")
    con.execute("""
        CREATE TABLE ce_catalogos_actividades (
            codigo VARCHAR, descripcion VARCHAR, clasificador VARCHAR
        )
    """)
    con.executemany("INSERT INTO ce_catalogos_actividades VALUES (?, ?, ?)", filas)
    print(f"  ce_catalogos_actividades: {len(filas)} filas")


def crear_ce_diccionarios_datos(con: duckdb.DuckDBPyConnection) -> None:
    """Crea diccionario de datos (~15 filas: 10 indicadores + 5 metadatos)."""
    filas = []
    for anio in ANIOS:
        for nombre, desc in DICCIONARIO_INDICADORES.items():
            if isinstance(desc, tuple):
                descripcion, tipo = desc
            else:
                descripcion, tipo = desc, "numérico"
            filas.append((nombre, descripcion, tipo, anio))
        # Filas de metadatos que el staging debe filtrar
        for nombre, descripcion, tipo in METADATOS_DICCIONARIO:
            filas.append((nombre, descripcion, tipo, anio))

    con.execute("DROP TABLE IF EXISTS ce_diccionarios_datos")
    con.execute("""
        CREATE TABLE ce_diccionarios_datos (
            nombre_columna VARCHAR, descripcion VARCHAR,
            tipo_dato VARCHAR, anio INTEGER
        )
    """)
    con.executemany("INSERT INTO ce_diccionarios_datos VALUES (?, ?, ?, ?)", filas)
    print(f"  ce_diccionarios_datos: {len(filas)} filas")


def crear_ce_datos(con: duckdb.DuckDBPyConnection) -> None:
    """Crea tabla principal del Censo Económico (~670 filas) con datos sucios."""
    filas = []
    id_counter = 1

    for anio in ANIOS:
        for codigo in CODIGOS_ACTIVIDAD:
            escala_base = 1.0 if anio == 2024 else 0.85

            # Nivel nacional (entidad='', municipio='')
            vals = _generar_indicadores(escala_base * 10)
            # Dato sucio: a veces string vacío, a veces espacios
            ent_vacio = random.choice(["", "  ", ""])
            mun_vacio = random.choice(["", " ", ""])
            filas.append((id_counter, anio, ent_vacio, mun_vacio, codigo, *vals.values()))
            id_counter += 1

            # Nivel entidad
            for cve_ent, _ in ENTIDADES:
                vals = _generar_indicadores(escala_base * 2)
                # Dato sucio: espacios extra en cve_ent
                ent = f" {cve_ent}" if random.random() < 0.15 else cve_ent
                filas.append((id_counter, anio, ent, "", codigo, *vals.values()))
                id_counter += 1

                # Nivel municipio
                for cve_mun, _ in MUNICIPIOS[cve_ent]:
                    vals = _generar_indicadores(escala_base)
                    # Dato sucio: espacios en municipio
                    mun = f"{cve_mun} " if random.random() < 0.15 else cve_mun
                    filas.append((id_counter, anio, cve_ent, mun, codigo, *vals.values()))
                    id_counter += 1

    columnas_ind = ", ".join(f"{ind} INTEGER" for ind in INDICADORES_CE)
    con.execute("DROP TABLE IF EXISTS ce_datos")
    con.execute(f"""
        CREATE TABLE ce_datos (
            id INTEGER, anio INTEGER, entidad VARCHAR, municipio VARCHAR,
            codigo VARCHAR, {columnas_ind}
        )
    """)
    placeholders = ", ".join(["?"] * (5 + len(INDICADORES_CE)))
    con.executemany(f"INSERT INTO ce_datos VALUES ({placeholders})", filas)
    print(f"  ce_datos: {len(filas)} filas")


def crear_dof_presupuesto(con: duckdb.DuckDBPyConnection) -> None:
    """Crea tabla de presupuesto DOF (~120 filas) con nombres sucios y filas basura."""
    filas = []
    id_counter = 1

    for anio in ANIOS:
        for ramo, anexo, fondo in FONDOS_DOF:
            # Filas válidas por entidad
            for cve_ent, _ in ENTIDADES:
                nombre_sucio = random.choice(NOMBRES_SUCIOS[cve_ent])
                anual = _valor_aleatorio(500000)
                meses = [_valor_aleatorio(anual // 12) for _ in range(12)]
                filas.append((id_counter, anio, ramo, anexo, fondo, nombre_sucio, False, False, anual, *meses))
                id_counter += 1

            # Fila total por fondo
            anual_total = _valor_aleatorio(3000000)
            meses_total = [_valor_aleatorio(anual_total // 12) for _ in range(12)]
            filas.append((id_counter, anio, ramo, anexo, fondo, "TOTAL", True, False, anual_total, *meses_total))
            id_counter += 1

            # Fila consolidado (solo para algunos fondos)
            if random.random() < 0.4:
                anual_cons = _valor_aleatorio(2800000)
                meses_cons = [_valor_aleatorio(anual_cons // 12) for _ in range(12)]
                filas.append(
                    (
                        id_counter,
                        anio,
                        ramo,
                        anexo,
                        fondo,
                        "Consolidado Nacional *",
                        False,
                        True,
                        anual_cons,
                        *meses_cons,
                    )
                )
                id_counter += 1

    meses_cols = ", ".join(f"{m} DOUBLE" for m in MESES)
    con.execute("DROP TABLE IF EXISTS dof_presupuesto")
    con.execute(f"""
        CREATE TABLE dof_presupuesto (
            id INTEGER, anio INTEGER, ramo INTEGER, anexo INTEGER,
            fondo VARCHAR, entidad VARCHAR,
            es_total BOOLEAN, es_consolidado BOOLEAN,
            anual DOUBLE, {meses_cols}
        )
    """)
    placeholders = ", ".join(["?"] * (9 + 12))
    con.executemany(f"INSERT INTO dof_presupuesto VALUES ({placeholders})", filas)
    print(f"  dof_presupuesto: {len(filas)} filas")


def main() -> None:
    """Genera la base de datos sintética completa."""
    # Eliminar BD existente para regenerar limpio
    if BD_DESTINO.exists():
        BD_DESTINO.unlink()

    print(f"Generando base de datos en {BD_DESTINO}...")
    con = duckdb.connect(str(BD_DESTINO))

    print("\nCenso Económico:")
    crear_ce_catalogos_entidades_municipios(con)
    crear_ce_catalogos_actividades(con)
    crear_ce_diccionarios_datos(con)
    crear_ce_datos(con)

    print("\nPresupuesto DOF:")
    crear_dof_presupuesto(con)

    # Verificar conteos
    print("\n── Verificación ──")
    for tabla in [
        "ce_datos",
        "ce_catalogos_actividades",
        "ce_catalogos_entidades_municipios",
        "ce_diccionarios_datos",
        "dof_presupuesto",
    ]:
        conteo = con.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]  # type: ignore[index]
        print(f"  {tabla}: {conteo} filas")

    con.close()
    print(f"\nBase de datos generada exitosamente: {BD_DESTINO}")


if __name__ == "__main__":
    main()
