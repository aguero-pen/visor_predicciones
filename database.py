import os
import csv
import sys
import hashlib
import psycopg2

csv.field_size_limit(sys.maxsize)
from psycopg2.extras import RealDictCursor, execute_values
from datetime import datetime
from pathlib import Path

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://visor:visor_pass_2026@localhost:5432/visor_predicciones")


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            rol TEXT NOT NULL CHECK(rol IN ('admin', 'tagger'))
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS intervenciones (
            id SERIAL PRIMARY KEY,
            titulo_acta TEXT,
            tipo_sesion TEXT,
            nsesion TEXT,
            fecha TEXT,
            legislatura TEXT,
            sesion TEXT,
            preside TEXT,
            preside_puesto TEXT,
            parte_acta TEXT,
            seccion_actual TEXT,
            nombre_diputado TEXT,
            intervencion TEXT,
            tema_1_pobreza_y_desigualdad REAL,
            tema_2_economia_y_empleo REAL,
            tema_3_gestion_politica REAL,
            tema_4_medio_ambiente REAL,
            tema_5_solvencia_del_estado REAL,
            tema_6_convivencia_social REAL,
            tema_7_otro REAL,
            subtema_2_derechos_humanos REAL,
            subtema_3_educacion REAL,
            subtema_4_ingresos_y_salarios REAL,
            subtema_5_inversion_social REAL,
            subtema_6_mujeres REAL,
            subtema_7_personas_con_discapacidad REAL,
            subtema_8_pobreza_y_desigualdad REAL,
            subtema_9_salud_seguridad_social REAL,
            subtema_10_seguridad_ciudadana REAL,
            subtema_11_vivienda REAL,
            subtema_12_agricultura_y_agropecuario REAL,
            subtema_13_banca_y_finanzas REAL,
            subtema_14_comercio_exterior REAL,
            subtema_15_crecimiento_competitividad_productividad REAL,
            subtema_16_empleo_derechos_trabajadores REAL,
            subtema_17_industria_y_comercio REAL,
            subtema_18_infraestructura REAL,
            subtema_19_politica_monetaria REAL,
            subtema_20_pymes_emprendedurismo_e_innovacion REAL,
            subtema_21_servicios REAL,
            subtema_22_telecomunicaciones_nuevas_tecnologias REAL,
            subtema_23_turismo REAL,
            subtema_24_administracion_de_justicia REAL,
            subtema_25_conflictividad_social REAL,
            subtema_26_controles_politicos_juridicos_admin REAL,
            subtema_27_corrupcion_transparencia_gob_abierto REAL,
            subtema_28_financiamiento_partidos_politicos REAL,
            subtema_29_generacion_de_alianzas_y_acuerdos REAL,
            subtema_30_gobiernos_locales REAL,
            subtema_31_nombramientos_de_funcionarios REAL,
            subtema_32_politica_exterior REAL,
            subtema_33_proceso_legislativo REAL,
            subtema_34_reforma_del_estado_y_politica REAL,
            subtema_35_bienestar_animal REAL,
            subtema_36_conservacion REAL,
            subtema_37_planificacion REAL,
            subtema_38_seguridad_alimentaria REAL,
            subtema_39_uso_de_los_recursos REAL,
            subtema_40_administracion_tributaria REAL,
            subtema_41_empleo_publico_convenciones_colect REAL,
            subtema_42_exoneraciones REAL,
            subtema_43_gasto_y_finanzas_publicas REAL,
            subtema_44_impuestos REAL,
            subtema_45_cultura REAL,
            subtema_46_deporte_y_recreacion REAL,
            subtema_47_familia REAL,
            subtema_48_religion REAL,
            subtema_49_valores_y_etica REAL,
            subtema_99_ninguno REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS validaciones (
            id SERIAL PRIMARY KEY,
            row_index INTEGER NOT NULL,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
            tema_predicho TEXT NOT NULL,
            subtema_predicho TEXT NOT NULL,
            es_correcto BOOLEAN NOT NULL,
            tema_correcto TEXT,
            subtema_correcto TEXT,
            notas TEXT,
            tiempo_segundos REAL,
            fecha_validacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(row_index, usuario_id)
        )
    """)
    # Migración: agregar tiempo_segundos si no existe
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'validaciones' AND column_name = 'tiempo_segundos'
    """)
    if not cur.fetchone():
        cur.execute("ALTER TABLE validaciones ADD COLUMN tiempo_segundos REAL")

    # Crear admin por defecto si no existe
    cur.execute("SELECT id FROM usuarios WHERE username = 'admin'")
    if not cur.fetchone():
        cur.execute("INSERT INTO usuarios (username, password_hash, rol) VALUES (%s, %s, %s)",
                    ("admin", _hash_password("admin"), "admin"))
    conn.commit()
    cur.close()
    conn.close()


# --- Intervenciones ---

META_COLS = ["titulo_acta", "tipo_sesion", "nsesion", "fecha", "legislatura", "sesion",
             "preside", "preside_puesto", "parte_acta", "seccion_actual", "nombre_diputado", "intervencion"]

PROB_COLS = [
    "TEMA_1_POBREZA_Y_DESIGUALDAD", "TEMA_2_ECONOMIA_Y_EMPLEO", "TEMA_3_GESTION_POLITICA",
    "TEMA_4_MEDIO_AMBIENTE", "TEMA_5_SOLVENCIA_DEL_ESTADO", "TEMA_6_CONVIVENCIA_SOCIAL", "TEMA_7_OTRO",
    "SUBTEMA_2_DERECHOS_HUMANOS", "SUBTEMA_3_EDUCACION", "SUBTEMA_4_INGRESOS_Y_SALARIOS",
    "SUBTEMA_5_INVERSION_SOCIAL", "SUBTEMA_6_MUJERES", "SUBTEMA_7_PERSONAS_CON_DISCAPACIDAD",
    "SUBTEMA_8_POBREZA_Y_DESIGUALDAD", "SUBTEMA_9_SALUD_SEGURIDAD_SOCIAL",
    "SUBTEMA_10_SEGURIDAD_CIUDADANA", "SUBTEMA_11_VIVIENDA",
    "SUBTEMA_12_AGRICULTURA_Y_AGROPECUARIO", "SUBTEMA_13_BANCA_Y_FINANZAS", "SUBTEMA_14_COMERCIO_EXTERIOR",
    "SUBTEMA_15_CRECIMIENTO_COMPETITIVIDAD_PRODUCTIVIDAD", "SUBTEMA_16_EMPLEO_DERECHOS_TRABAJADORES",
    "SUBTEMA_17_INDUSTRIA_Y_COMERCIO", "SUBTEMA_18_INFRAESTRUCTURA", "SUBTEMA_19_POLITICA_MONETARIA",
    "SUBTEMA_20_PYMES_EMPRENDEDURISMO_E_INNOVACION", "SUBTEMA_21_SERVICIOS",
    "SUBTEMA_22_TELECOMUNICACIONES_NUEVAS_TECNOLOGIAS", "SUBTEMA_23_TURISMO",
    "SUBTEMA_24_ADMINISTRACION_DE_JUSTICIA", "SUBTEMA_25_CONFLICTIVIDAD_SOCIAL",
    "SUBTEMA_26_CONTROLES_POLITICOS_JURIDICOS_ADMIN", "SUBTEMA_27_CORRUPCION_TRANSPARENCIA_GOB_ABIERTO",
    "SUBTEMA_28_FINANCIAMIENTO_PARTIDOS_POLITICOS", "SUBTEMA_29_GENERACION_DE_ALIANZAS_Y_ACUERDOS",
    "SUBTEMA_30_GOBIERNOS_LOCALES", "SUBTEMA_31_NOMBRAMIENTOS_DE_FUNCIONARIOS",
    "SUBTEMA_32_POLITICA_EXTERIOR", "SUBTEMA_33_PROCESO_LEGISLATIVO", "SUBTEMA_34_REFORMA_DEL_ESTADO_Y_POLITICA",
    "SUBTEMA_35_BIENESTAR_ANIMAL", "SUBTEMA_36_CONSERVACION", "SUBTEMA_37_PLANIFICACION",
    "SUBTEMA_38_SEGURIDAD_ALIMENTARIA", "SUBTEMA_39_USO_DE_LOS_RECURSOS",
    "SUBTEMA_40_ADMINISTRACION_TRIBUTARIA", "SUBTEMA_41_EMPLEO_PUBLICO_CONVENCIONES_COLECT",
    "SUBTEMA_42_EXONERACIONES", "SUBTEMA_43_GASTO_Y_FINANZAS_PUBLICAS", "SUBTEMA_44_IMPUESTOS",
    "SUBTEMA_45_CULTURA", "SUBTEMA_46_DEPORTE_Y_RECREACION", "SUBTEMA_47_FAMILIA",
    "SUBTEMA_48_RELIGION", "SUBTEMA_49_VALORES_Y_ETICA", "SUBTEMA_99_NINGUNO",
]

ALL_CSV_COLS = META_COLS + PROB_COLS


def cargar_csv_a_db(csv_path: str):
    """Carga el CSV a la tabla intervenciones si está vacía."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM intervenciones")
    if cur.fetchone()[0] > 0:
        cur.close()
        conn.close()
        return

    db_cols = [c.lower() for c in ALL_CSV_COLS]
    placeholders = ",".join(["%s"] * len(db_cols))
    insert_sql = f"INSERT INTO intervenciones ({','.join(db_cols)}) VALUES ({placeholders})"

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            values = []
            for col in ALL_CSV_COLS:
                val = row.get(col, "")
                if col in PROB_COLS:
                    values.append(float(val) if val else 0.0)
                else:
                    values.append(val)
            batch.append(tuple(values))
            if len(batch) >= 500:
                cur.executemany(insert_sql, batch)
                batch = []
        if batch:
            cur.executemany(insert_sql, batch)

    conn.commit()
    cur.close()
    conn.close()


def contar_intervenciones():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM intervenciones")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()
    return total


def obtener_intervencion(row_index: int):
    """Obtiene una intervención por su índice (id - 1 = row_index)."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM intervenciones WHERE id = %s", (row_index + 1,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    result = dict(row)
    # Convertir claves de probabilidad a mayúsculas para compatibilidad con app.py
    converted = {}
    for k, v in result.items():
        upper_key = k.upper()
        # Buscar si es una columna de probabilidad
        if upper_key in PROB_COLS:
            converted[upper_key] = v
        else:
            converted[k] = v
    return converted


def obtener_temas_principales():
    """Devuelve dict {row_index: tema_principal} para todas las intervenciones."""
    conn = get_connection()
    cur = conn.cursor()
    tema_cols = [
        "tema_1_pobreza_y_desigualdad", "tema_2_economia_y_empleo", "tema_3_gestion_politica",
        "tema_4_medio_ambiente", "tema_5_solvencia_del_estado", "tema_6_convivencia_social", "tema_7_otro"
    ]
    tema_labels = [
        "TEMA_1_POBREZA_Y_DESIGUALDAD", "TEMA_2_ECONOMIA_Y_EMPLEO", "TEMA_3_GESTION_POLITICA",
        "TEMA_4_MEDIO_AMBIENTE", "TEMA_5_SOLVENCIA_DEL_ESTADO", "TEMA_6_CONVIVENCIA_SOCIAL", "TEMA_7_OTRO"
    ]
    cols_sql = ", ".join(tema_cols)
    cur.execute(f"SELECT id, {cols_sql} FROM intervenciones")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    result = {}
    for row in rows:
        row_index = row[0] - 1
        probs = row[1:]
        max_idx = max(range(len(probs)), key=lambda i: probs[i] or 0)
        result[row_index] = tema_labels[max_idx]
    return result


# --- Usuarios ---

def crear_usuario(username: str, password: str, rol: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO usuarios (username, password_hash, rol) VALUES (%s, %s, %s)",
                (username, _hash_password(password), rol))
    conn.commit()
    cur.close()
    conn.close()


def autenticar_usuario(username: str, password: str):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM usuarios WHERE username = %s AND password_hash = %s",
                (username, _hash_password(password)))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return dict(user) if user else None


def obtener_usuario(user_id: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM usuarios WHERE id = %s", (user_id,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return dict(user) if user else None


def listar_usuarios():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, username, rol FROM usuarios ORDER BY rol, username")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]


def cambiar_password(user_id: int, new_password: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE usuarios SET password_hash = %s WHERE id = %s",
                (_hash_password(new_password), user_id))
    conn.commit()
    cur.close()
    conn.close()


def eliminar_usuario(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM validaciones WHERE usuario_id = %s", (user_id,))
    cur.execute("DELETE FROM usuarios WHERE id = %s", (user_id,))
    conn.commit()
    cur.close()
    conn.close()


def obtener_filtros_explorar():
    """Devuelve los valores únicos para los filtros del explorador."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT SUBSTRING(fecha,1,4) FROM intervenciones ORDER BY 1")
    anios = [r[0] for r in cur.fetchall() if r[0]]
    cur.execute("SELECT DISTINCT SUBSTRING(fecha,1,7) FROM intervenciones ORDER BY 1")
    meses = [r[0] for r in cur.fetchall() if r[0]]
    cur.execute("SELECT tipo_sesion, nsesion FROM intervenciones GROUP BY tipo_sesion, nsesion ORDER BY tipo_sesion, MIN(CAST(nsesion AS INTEGER))")
    sesiones = [f"{r[0]} #{r[1]}" for r in cur.fetchall() if r[0]]
    cur.close()
    conn.close()
    return {"anios": anios, "meses": meses, "sesiones": sesiones}


def _build_tema_subtema_cases():
    """Construye los CASE SQL para tema y subtema principal."""
    tema_cols = [
        "tema_1_pobreza_y_desigualdad", "tema_2_economia_y_empleo", "tema_3_gestion_politica",
        "tema_4_medio_ambiente", "tema_5_solvencia_del_estado", "tema_6_convivencia_social", "tema_7_otro"
    ]
    tema_labels = [
        "TEMA_1_POBREZA_Y_DESIGUALDAD", "TEMA_2_ECONOMIA_Y_EMPLEO", "TEMA_3_GESTION_POLITICA",
        "TEMA_4_MEDIO_AMBIENTE", "TEMA_5_SOLVENCIA_DEL_ESTADO", "TEMA_6_CONVIVENCIA_SOCIAL", "TEMA_7_OTRO"
    ]
    greatest = "GREATEST(" + ",".join(tema_cols) + ")"
    case_parts = " ".join(f"WHEN {col} = {greatest} THEN '{label}'" for col, label in zip(tema_cols, tema_labels))
    tema_case = f"CASE {case_parts} END"
    return tema_case, tema_labels


SUBTHEME_MAP_DB = {
    "TEMA_1_POBREZA_Y_DESIGUALDAD": [
        "SUBTEMA_2_DERECHOS_HUMANOS", "SUBTEMA_3_EDUCACION", "SUBTEMA_4_INGRESOS_Y_SALARIOS",
        "SUBTEMA_5_INVERSION_SOCIAL", "SUBTEMA_6_MUJERES", "SUBTEMA_7_PERSONAS_CON_DISCAPACIDAD",
        "SUBTEMA_8_POBREZA_Y_DESIGUALDAD", "SUBTEMA_9_SALUD_SEGURIDAD_SOCIAL",
        "SUBTEMA_10_SEGURIDAD_CIUDADANA", "SUBTEMA_11_VIVIENDA"
    ],
    "TEMA_2_ECONOMIA_Y_EMPLEO": [
        "SUBTEMA_12_AGRICULTURA_Y_AGROPECUARIO", "SUBTEMA_13_BANCA_Y_FINANZAS", "SUBTEMA_14_COMERCIO_EXTERIOR",
        "SUBTEMA_15_CRECIMIENTO_COMPETITIVIDAD_PRODUCTIVIDAD", "SUBTEMA_16_EMPLEO_DERECHOS_TRABAJADORES",
        "SUBTEMA_17_INDUSTRIA_Y_COMERCIO", "SUBTEMA_18_INFRAESTRUCTURA", "SUBTEMA_19_POLITICA_MONETARIA",
        "SUBTEMA_20_PYMES_EMPRENDEDURISMO_E_INNOVACION", "SUBTEMA_21_SERVICIOS",
        "SUBTEMA_22_TELECOMUNICACIONES_NUEVAS_TECNOLOGIAS", "SUBTEMA_23_TURISMO"
    ],
    "TEMA_3_GESTION_POLITICA": [
        "SUBTEMA_24_ADMINISTRACION_DE_JUSTICIA", "SUBTEMA_25_CONFLICTIVIDAD_SOCIAL",
        "SUBTEMA_26_CONTROLES_POLITICOS_JURIDICOS_ADMIN", "SUBTEMA_27_CORRUPCION_TRANSPARENCIA_GOB_ABIERTO",
        "SUBTEMA_28_FINANCIAMIENTO_PARTIDOS_POLITICOS", "SUBTEMA_29_GENERACION_DE_ALIANZAS_Y_ACUERDOS",
        "SUBTEMA_30_GOBIERNOS_LOCALES", "SUBTEMA_31_NOMBRAMIENTOS_DE_FUNCIONARIOS",
        "SUBTEMA_32_POLITICA_EXTERIOR", "SUBTEMA_33_PROCESO_LEGISLATIVO", "SUBTEMA_34_REFORMA_DEL_ESTADO_Y_POLITICA"
    ],
    "TEMA_4_MEDIO_AMBIENTE": [
        "SUBTEMA_35_BIENESTAR_ANIMAL", "SUBTEMA_36_CONSERVACION", "SUBTEMA_37_PLANIFICACION",
        "SUBTEMA_38_SEGURIDAD_ALIMENTARIA", "SUBTEMA_39_USO_DE_LOS_RECURSOS"
    ],
    "TEMA_5_SOLVENCIA_DEL_ESTADO": [
        "SUBTEMA_40_ADMINISTRACION_TRIBUTARIA", "SUBTEMA_41_EMPLEO_PUBLICO_CONVENCIONES_COLECT",
        "SUBTEMA_42_EXONERACIONES", "SUBTEMA_43_GASTO_Y_FINANZAS_PUBLICAS", "SUBTEMA_44_IMPUESTOS"
    ],
    "TEMA_6_CONVIVENCIA_SOCIAL": [
        "SUBTEMA_45_CULTURA", "SUBTEMA_46_DEPORTE_Y_RECREACION", "SUBTEMA_47_FAMILIA",
        "SUBTEMA_48_RELIGION", "SUBTEMA_49_VALORES_Y_ETICA"
    ],
    "TEMA_7_OTRO": ["SUBTEMA_99_NINGUNO"],
}


def _build_subtema_case(tema_label: str):
    """Construye CASE SQL para subtema dado un tema."""
    subtemas = SUBTHEME_MAP_DB.get(tema_label, [])
    if not subtemas:
        return None, []
    sub_cols = [s.lower() for s in subtemas]
    greatest = "GREATEST(" + ",".join(sub_cols) + ")"
    case_parts = " ".join(f"WHEN {col} = {greatest} THEN '{label}'" for col, label in zip(sub_cols, subtemas))
    return f"CASE {case_parts} END", subtemas


def _build_subtema_top_case():
    """Construye CASE SQL para obtener el subtema top según el tema principal."""
    tema_case, _ = _build_tema_subtema_cases()
    branches = []
    for tema_label, subtemas in SUBTHEME_MAP_DB.items():
        sub_cols = [s.lower() for s in subtemas]
        greatest = "GREATEST(" + ",".join(sub_cols) + ")"
        inner_case = " ".join(f"WHEN {col} = {greatest} THEN '{label}'" for col, label in zip(sub_cols, subtemas))
        branches.append(f"WHEN {tema_case} = '{tema_label}' THEN (CASE {inner_case} END)")
    return "CASE " + " ".join(branches) + " END"


def explorar_intervenciones(tema=None, subtema=None, anio=None, mes=None, sesion=None, diputado=None, q_diputado=None, q_texto=None, page=1, per_page=20):
    """Busca intervenciones con filtros y paginación."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    tema_case, tema_labels = _build_tema_subtema_cases()

    # Construir WHERE
    conditions = []
    params = []
    if tema:
        conditions.append(f"{tema_case} = %s")
        params.append(tema)
    if subtema and tema:
        subtema_case, _ = _build_subtema_case(tema)
        if subtema_case:
            conditions.append(f"{subtema_case} = %s")
            params.append(subtema)
    if anio:
        conditions.append("SUBSTRING(fecha,1,4) = %s")
        params.append(anio)
    if mes:
        conditions.append("SUBSTRING(fecha,1,7) = %s")
        params.append(mes)
    if sesion:
        parts = sesion.rsplit(" #", 1)
        if len(parts) == 2:
            conditions.append("tipo_sesion = %s AND nsesion = %s")
            params.extend(parts)
    if diputado:
        conditions.append("nombre_diputado = %s")
        params.append(diputado)
    if q_diputado:
        conditions.append("nombre_diputado ILIKE %s")
        params.append(f"%{q_diputado}%")
    if q_texto:
        conditions.append("intervencion ILIKE %s")
        params.append(f"%{q_texto}%")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # Params base (sin tema ni subtema) para conteos
    params_base = []
    conditions_base = []
    if anio:
        conditions_base.append("SUBSTRING(fecha,1,4) = %s")
        params_base.append(anio)
    if mes:
        conditions_base.append("SUBSTRING(fecha,1,7) = %s")
        params_base.append(mes)
    if sesion:
        parts = sesion.rsplit(" #", 1)
        if len(parts) == 2:
            conditions_base.append("tipo_sesion = %s AND nsesion = %s")
            params_base.extend(parts)
    if diputado:
        conditions_base.append("nombre_diputado = %s")
        params_base.append(diputado)
    if q_diputado:
        conditions_base.append("nombre_diputado ILIKE %s")
        params_base.append(f"%{q_diputado}%")
    if q_texto:
        conditions_base.append("intervencion ILIKE %s")
        params_base.append(f"%{q_texto}%")

    where_base = ("WHERE " + " AND ".join(conditions_base)) if conditions_base else ""

    # Conteos por tema
    cur.execute(f"SELECT {tema_case} as tema_principal, COUNT(*) as cnt FROM intervenciones {where_base} GROUP BY tema_principal ORDER BY cnt DESC", params_base)
    conteos = [dict(r) for r in cur.fetchall()]

    # Conteos por subtema (solo si hay tema seleccionado)
    conteos_subtema = []
    if tema:
        subtema_case, _ = _build_subtema_case(tema)
        if subtema_case:
            # Filtro base + tema
            params_tema = list(params_base)
            conds_tema = list(conditions_base) + [f"{tema_case} = %s"]
            params_tema.append(tema)
            where_tema = "WHERE " + " AND ".join(conds_tema)
            cur.execute(f"SELECT {subtema_case} as subtema_principal, COUNT(*) as cnt FROM intervenciones {where_tema} GROUP BY subtema_principal ORDER BY cnt DESC", params_tema)
            conteos_subtema = [dict(r) for r in cur.fetchall()]

    # Conteos por año
    cur.execute(f"SELECT SUBSTRING(fecha,1,4) as anio_val, COUNT(*) as cnt FROM intervenciones {where_base} GROUP BY anio_val ORDER BY anio_val", params_base)
    conteos_anio = [dict(r) for r in cur.fetchall()]

    # Total con filtros
    cur.execute(f"SELECT COUNT(*) as total FROM intervenciones {where}", params)
    total = cur.fetchone()["total"]

    # Items paginados
    subtema_top_case = _build_subtema_top_case()
    offset = (page - 1) * per_page
    cur.execute(f"""
        SELECT id, nombre_diputado, fecha, tipo_sesion, nsesion,
               LEFT(intervencion, 300) as intervencion_corta,
               {tema_case} as tema_principal,
               {subtema_top_case} as subtema_principal
        FROM intervenciones {where}
        ORDER BY id
        LIMIT %s OFFSET %s
    """, params + [per_page, offset])
    items = [dict(r) for r in cur.fetchall()]

    cur.close()
    conn.close()
    return items, total, conteos, conteos_subtema, conteos_anio


def borrar_todas_validaciones():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM validaciones")
    conn.commit()
    cur.close()
    conn.close()


# --- Validaciones ---

def guardar_validacion(row_index: int, usuario_id: int, tema_predicho: str, subtema_predicho: str,
                       es_correcto: bool, tema_correcto: str = None,
                       subtema_correcto: str = None, notas: str = None, tiempo_segundos: float = None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO validaciones (row_index, usuario_id, tema_predicho, subtema_predicho, es_correcto,
                                  tema_correcto, subtema_correcto, notas, tiempo_segundos, fecha_validacion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(row_index, usuario_id) DO UPDATE SET
            tema_predicho = EXCLUDED.tema_predicho,
            subtema_predicho = EXCLUDED.subtema_predicho,
            es_correcto = EXCLUDED.es_correcto,
            tema_correcto = EXCLUDED.tema_correcto,
            subtema_correcto = EXCLUDED.subtema_correcto,
            notas = EXCLUDED.notas,
            tiempo_segundos = EXCLUDED.tiempo_segundos,
            fecha_validacion = EXCLUDED.fecha_validacion
    """, (row_index, usuario_id, tema_predicho, subtema_predicho, es_correcto,
          tema_correcto, subtema_correcto, notas, tiempo_segundos, datetime.now().isoformat()))
    conn.commit()
    cur.close()
    conn.close()


def obtener_validacion(row_index: int, usuario_id: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM validaciones WHERE row_index = %s AND usuario_id = %s",
                (row_index, usuario_id))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None


def obtener_todas_validaciones(row_index: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT v.*, u.username FROM validaciones v
        JOIN usuarios u ON v.usuario_id = u.id
        WHERE v.row_index = %s
    """, (row_index,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]


def obtener_indices_validados_por_usuario(usuario_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT row_index FROM validaciones WHERE usuario_id = %s", (usuario_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {r[0] for r in rows}


def obtener_indices_con_validaciones():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT row_index, COUNT(*) as cnt FROM validaciones GROUP BY row_index")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {r[0]: r[1] for r in rows}


def obtener_consenso(row_index: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT es_correcto FROM validaciones WHERE row_index = %s", (row_index,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if len(rows) < 1:
        return None
    correctas = sum(1 for r in rows if r[0])
    return correctas > len(rows) / 2


def obtener_estadisticas(usuario_id: int = None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM validaciones WHERE usuario_id = %s", (usuario_id,))
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM validaciones WHERE usuario_id = %s AND es_correcto = true", (usuario_id,))
    correctas = cur.fetchone()[0]
    cur.close()
    conn.close()
    return {"validadas": total, "correctas": correctas, "incorrectas": total - correctas}


def obtener_estadisticas_global():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM validaciones")
    total_validaciones = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT row_index) FROM validaciones")
    items_tocados = cur.fetchone()[0]

    cur.execute("""
        SELECT row_index, SUM(CASE WHEN es_correcto THEN 1 ELSE 0 END) as si, COUNT(*) as total
        FROM validaciones GROUP BY row_index HAVING COUNT(*) >= 1
    """)
    consensos = cur.fetchall()
    con_consenso = len(consensos)
    consenso_correctas = sum(1 for r in consensos if r[1] > r[2] / 2)

    cur.execute("""
        SELECT u.id, u.username, COUNT(v.id) as total,
               SUM(CASE WHEN v.es_correcto THEN 1 ELSE 0 END) as correctas,
               AVG(v.tiempo_segundos) as tiempo_promedio,
               SUM(v.tiempo_segundos) as tiempo_total
        FROM usuarios u
        LEFT JOIN validaciones v ON u.id = v.usuario_id
        WHERE u.rol = 'tagger'
        GROUP BY u.id, u.username
        ORDER BY total DESC
    """)
    taggers = cur.fetchall()

    cur.close()
    conn.close()
    return {
        "total_validaciones": total_validaciones,
        "items_tocados": items_tocados,
        "con_consenso": con_consenso,
        "consenso_correctas": consenso_correctas,
        "consenso_incorrectas": con_consenso - consenso_correctas,
        "taggers": [{"id": t[0], "username": t[1], "total": t[2], "correctas": t[3],
                     "tiempo_promedio": t[4], "tiempo_total": t[5]} for t in taggers],
    }


def obtener_detalle_tagger(usuario_id: int):
    """Estadísticas detalladas de un tagger: tiempos, validaciones."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Info del usuario
    cur.execute("SELECT id, username, rol FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = dict(cur.fetchone())

    # Total, correctas, tiempos
    cur.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN es_correcto THEN 1 ELSE 0 END) as correctas,
               AVG(tiempo_segundos) as tiempo_promedio,
               SUM(tiempo_segundos) as tiempo_total,
               MIN(tiempo_segundos) as tiempo_min,
               MAX(tiempo_segundos) as tiempo_max,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tiempo_segundos) as tiempo_mediana
        FROM validaciones WHERE usuario_id = %s AND tiempo_segundos IS NOT NULL
    """, (usuario_id,))
    tiempos = dict(cur.fetchone())

    cur.execute("SELECT COUNT(*) as cnt FROM validaciones WHERE usuario_id = %s", (usuario_id,))
    tiempos["total_validaciones"] = cur.fetchone()["cnt"]

    # Últimas validaciones
    cur.execute("""
        SELECT v.row_index, v.es_correcto, v.tema_predicho, v.subtema_predicho,
               v.tema_correcto, v.subtema_correcto, v.tiempo_segundos, v.fecha_validacion, v.notas
        FROM validaciones v
        WHERE v.usuario_id = %s
        ORDER BY v.fecha_validacion DESC
        LIMIT 50
    """, (usuario_id,))
    ultimas = [dict(r) for r in cur.fetchall()]

    cur.close()
    conn.close()
    return {"usuario": usuario, "tiempos": tiempos, "ultimas": ultimas}


def obtener_metricas_por_clase():
    """Calcula precision, recall y F1 por tema y subtema usando consenso de validación."""
    conn = get_connection()
    cur = conn.cursor()

    # Obtener items con al menos 1 validación
    # Para cada item: tema_predicho, y si el consenso dice correcto o incorrecto + tema_correcto
    cur.execute("""
        SELECT v.row_index, v.tema_predicho, v.subtema_predicho,
               v.es_correcto, v.tema_correcto, v.subtema_correcto
        FROM validaciones v
    """)
    todas = cur.fetchall()
    cur.close()
    conn.close()

    # Agrupar por row_index
    por_item = {}
    for row in todas:
        ri = row[0]
        if ri not in por_item:
            por_item[ri] = []
        por_item[ri].append({
            "tema_predicho": row[1], "subtema_predicho": row[2],
            "es_correcto": row[3], "tema_correcto": row[4], "subtema_correcto": row[5]
        })

    # Calcular métricas por clase (tema)
    # TP: predicho=X, consenso=correcto
    # FP: predicho=X, consenso=incorrecto (el correcto era otro)
    # FN: correcto=X pero predicho era otro (consenso=incorrecto, tema_correcto=X)
    tema_tp = {}
    tema_fp = {}
    tema_fn = {}
    subtema_tp = {}
    subtema_fp = {}
    subtema_fn = {}

    for ri, vals in por_item.items():
        if len(vals) < 1:
            continue
        # Consenso por mayoría
        correctos = sum(1 for v in vals if v["es_correcto"])
        es_correcto = correctos > len(vals) / 2

        tema_pred = vals[0]["tema_predicho"]
        subtema_pred = vals[0]["subtema_predicho"]

        if es_correcto:
            tema_tp[tema_pred] = tema_tp.get(tema_pred, 0) + 1
            subtema_tp[subtema_pred] = subtema_tp.get(subtema_pred, 0) + 1
        else:
            tema_fp[tema_pred] = tema_fp.get(tema_pred, 0) + 1
            subtema_fp[subtema_pred] = subtema_fp.get(subtema_pred, 0) + 1
            # Buscar el tema correcto más votado entre los que dijeron incorrecto
            correctos_temas = [v["tema_correcto"] for v in vals if not v["es_correcto"] and v["tema_correcto"]]
            correctos_subtemas = [v["subtema_correcto"] for v in vals if not v["es_correcto"] and v["subtema_correcto"]]
            if correctos_temas:
                from collections import Counter
                tema_real = Counter(correctos_temas).most_common(1)[0][0]
                tema_fn[tema_real] = tema_fn.get(tema_real, 0) + 1
            if correctos_subtemas:
                from collections import Counter
                subtema_real = Counter(correctos_subtemas).most_common(1)[0][0]
                subtema_fn[subtema_real] = subtema_fn.get(subtema_real, 0) + 1

    def calc_metrics(tp_dict, fp_dict, fn_dict, all_labels):
        metrics = []
        for label in all_labels:
            tp = tp_dict.get(label, 0)
            fp = fp_dict.get(label, 0)
            fn = fn_dict.get(label, 0)
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
            support = tp + fn
            metrics.append({
                "label": label, "tp": tp, "fp": fp, "fn": fn,
                "precision": precision, "recall": recall, "f1": f1, "support": support
            })
        return [m for m in metrics if m["support"] > 0 or m["tp"] > 0 or m["fp"] > 0]

    all_temas = [
        "TEMA_1_POBREZA_Y_DESIGUALDAD", "TEMA_2_ECONOMIA_Y_EMPLEO", "TEMA_3_GESTION_POLITICA",
        "TEMA_4_MEDIO_AMBIENTE", "TEMA_5_SOLVENCIA_DEL_ESTADO", "TEMA_6_CONVIVENCIA_SOCIAL", "TEMA_7_OTRO"
    ]
    all_subtemas = list(set(
        list(tema_tp.keys()) + list(tema_fp.keys()) + list(tema_fn.keys()) +
        list(subtema_tp.keys()) + list(subtema_fp.keys()) + list(subtema_fn.keys())
    ))
    subtema_labels = [s for s in all_subtemas if s.startswith("SUBTEMA")]

    return {
        "temas": calc_metrics(tema_tp, tema_fp, tema_fn, all_temas),
        "subtemas": calc_metrics(subtema_tp, subtema_fp, subtema_fn, sorted(subtema_labels)),
    }
