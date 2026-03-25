import os
import random
from pathlib import Path

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeSerializer

from database import (
    init_db, cargar_csv_a_db, contar_intervenciones, obtener_intervencion,
    guardar_validacion, obtener_validacion, obtener_estadisticas,
    obtener_indices_validados_por_usuario, obtener_indices_con_validaciones,
    autenticar_usuario, obtener_usuario, crear_usuario, listar_usuarios,
    obtener_estadisticas_global, obtener_todas_validaciones, obtener_consenso,
    obtener_temas_principales, obtener_detalle_tagger, obtener_metricas_por_clase,
    cambiar_password, eliminar_usuario,
)

app = FastAPI(title="Visor de Predicciones")

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

SECRET_KEY = os.environ.get("SECRET_KEY", "visor-predicciones-secret-key-2026")
serializer = URLSafeSerializer(SECRET_KEY)

# --- Jerarquía tema → subtemas ---
MAIN_THEMES = [
    "TEMA_1_POBREZA_Y_DESIGUALDAD", "TEMA_2_ECONOMIA_Y_EMPLEO", "TEMA_3_GESTION_POLITICA",
    "TEMA_4_MEDIO_AMBIENTE", "TEMA_5_SOLVENCIA_DEL_ESTADO", "TEMA_6_CONVIVENCIA_SOCIAL", "TEMA_7_OTRO"
]

SUBTHEME_MAP = {
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

ALL_SUBTEMAS = [s for subs in SUBTHEME_MAP.values() for s in subs]


def obtener_prediccion_top(row: dict):
    tema_probs = {t: float(row.get(t, 0)) for t in MAIN_THEMES}
    tema_top = max(tema_probs, key=tema_probs.get)

    subtemas_del_tema = SUBTHEME_MAP[tema_top]
    subtema_probs = {s: float(row.get(s, 0)) for s in subtemas_del_tema}
    subtema_top = max(subtema_probs, key=subtema_probs.get)

    all_temas = sorted(tema_probs.items(), key=lambda x: x[1], reverse=True)
    temas_con_subtemas = []
    for t, t_prob in all_temas:
        subs = SUBTHEME_MAP[t]
        sub_probs = sorted(
            [(s, float(row.get(s, 0))) for s in subs],
            key=lambda x: x[1], reverse=True
        )
        temas_con_subtemas.append({"tema": t, "prob": t_prob, "subtemas": sub_probs})

    return {
        "tema": tema_top,
        "tema_prob": tema_probs[tema_top],
        "subtema": subtema_top,
        "subtema_prob": subtema_probs[subtema_top],
        "temas_con_subtemas": temas_con_subtemas,
    }


def formato_label(label: str) -> str:
    parts = label.split("_", 2)
    if len(parts) >= 3:
        return parts[2].replace("_", " ").capitalize()
    return label


# Cache de temas principales (se carga una vez)
_temas_cache = {}

def cargar_temas_cache():
    global _temas_cache
    _temas_cache = obtener_temas_principales()


# --- Autenticación ---

def get_current_user(request: Request):
    cookie = request.cookies.get("session")
    if not cookie:
        return None
    try:
        user_id = serializer.loads(cookie)
        return obtener_usuario(user_id)
    except Exception:
        return None


# --- Rutas ---

@app.on_event("startup")
def startup():
    init_db()
    csv_path = BASE_DIR / "predicciones.csv"
    if csv_path.exists():
        cargar_csv_a_db(str(csv_path))
    cargar_temas_cache()


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {"error": None})


@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    user = autenticar_usuario(username, password)
    if not user:
        return templates.TemplateResponse(request, "login.html", {"error": "Usuario o contraseña incorrectos"})
    response = RedirectResponse("/", status_code=302)
    response.set_cookie("session", serializer.dumps(user["id"]), httponly=True, max_age=86400 * 7)
    return response


@app.get("/logout")
def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("session")
    return response


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    total = contar_intervenciones()

    if user["rol"] == "admin":
        stats = obtener_estadisticas_global()
    else:
        stats = obtener_estadisticas(usuario_id=user["id"])

    return templates.TemplateResponse(request, "index.html", {
        "user": user,
        "total": total,
        "stats": stats,
    })


@app.get("/revisar/{row_index}", response_class=HTMLResponse)
def revisar(request: Request, row_index: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    total = contar_intervenciones()
    if row_index < 0 or row_index >= total:
        return RedirectResponse("/", status_code=302)

    row = obtener_intervencion(row_index)
    if not row:
        return RedirectResponse("/", status_code=302)

    pred = obtener_prediccion_top(row)
    validacion = obtener_validacion(row_index, user["id"])
    todas = obtener_todas_validaciones(row_index)
    consenso = obtener_consenso(row_index)

    return templates.TemplateResponse(request, "revisar.html", {
        "user": user,
        "row_index": row_index,
        "row": row,
        "pred": pred,
        "validacion": validacion,
        "total": total,
        "main_themes": MAIN_THEMES,
        "subtheme_map": SUBTHEME_MAP,
        "formato_label": formato_label,
        "todas_validaciones": todas,
        "consenso": consenso,
        "num_validaciones": len(todas),
    })


@app.post("/validar")
def validar(
    request: Request,
    row_index: int = Form(...),
    tema_predicho: str = Form(...),
    subtema_predicho: str = Form(...),
    es_correcto: int = Form(...),
    tema_correcto: str = Form(None),
    subtema_correcto: str = Form(None),
    notas: str = Form(None),
    tiempo_segundos: float = Form(None),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    guardar_validacion(
        row_index=row_index,
        usuario_id=user["id"],
        tema_predicho=tema_predicho,
        subtema_predicho=subtema_predicho,
        es_correcto=bool(es_correcto),
        tema_correcto=tema_correcto if not es_correcto else None,
        subtema_correcto=subtema_correcto if not es_correcto else None,
        notas=notas if notas else None,
        tiempo_segundos=tiempo_segundos,
    )
    siguiente = seleccionar_pendiente_balanceado(user["id"])
    if siguiente is None:
        return RedirectResponse("/", status_code=302)
    return RedirectResponse(f"/revisar/{siguiente}", status_code=302)


def _contar_validados_por_tema(mis_validados: set, total: int) -> dict:
    resultado = {}
    for i in mis_validados:
        if i < total:
            tema = _temas_cache.get(i, MAIN_THEMES[0])
            resultado[tema] = resultado.get(tema, 0) + 1
    return resultado


def _elegir_tema_menos_validado(candidatos_por_tema: dict, mis_val_por_tema: dict) -> str:
    return min(
        candidatos_por_tema.keys(),
        key=lambda t: mis_val_por_tema.get(t, 0)
    )


def seleccionar_pendiente_balanceado(usuario_id: int):
    total = contar_intervenciones()
    mis_validados = obtener_indices_validados_por_usuario(usuario_id)
    validaciones_global = obtener_indices_con_validaciones()
    mis_val_por_tema = _contar_validados_por_tema(mis_validados, total)

    # Prioridad 1: items validados por otros pero no por mí (construir consenso)
    # Balanceado por tema: elige del tema con menos validaciones mías
    en_cola = [i for i in validaciones_global if i not in mis_validados and validaciones_global[i] < 3]
    if en_cola:
        cola_por_tema = {}
        for i in en_cola:
            tema = _temas_cache.get(i, MAIN_THEMES[0])
            cola_por_tema.setdefault(tema, []).append(i)

        mejor_tema = _elegir_tema_menos_validado(cola_por_tema, mis_val_por_tema)
        candidatos = cola_por_tema[mejor_tema]
        # Dentro del tema, preferir los que tienen más validaciones (consenso más rápido)
        candidatos.sort(key=lambda i: validaciones_global[i], reverse=True)
        max_val = validaciones_global[candidatos[0]]
        top = [i for i in candidatos if validaciones_global[i] == max_val]
        return random.choice(top)

    # Prioridad 2: items no validados por nadie, balanceado por tema
    pendientes_por_tema = {}
    for i in range(total):
        if i not in mis_validados and i not in validaciones_global:
            tema = _temas_cache.get(i, MAIN_THEMES[0])
            pendientes_por_tema.setdefault(tema, []).append(i)

    if not pendientes_por_tema:
        return None

    mejor_tema = _elegir_tema_menos_validado(pendientes_por_tema, mis_val_por_tema)
    return random.choice(pendientes_por_tema[mejor_tema])


@app.get("/pendiente")
def pendiente_aleatorio(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    siguiente = seleccionar_pendiente_balanceado(user["id"])
    if siguiente is None:
        return RedirectResponse("/", status_code=302)
    return RedirectResponse(f"/revisar/{siguiente}", status_code=302)


# --- Admin ---

@app.get("/admin/usuarios", response_class=HTMLResponse)
def admin_usuarios(request: Request):
    user = get_current_user(request)
    if not user or user["rol"] != "admin":
        return RedirectResponse("/", status_code=302)
    usuarios = listar_usuarios()
    return templates.TemplateResponse(request, "admin_usuarios.html", {
        "user": user,
        "usuarios": usuarios,
    })


@app.post("/admin/usuarios")
def admin_crear_usuario(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    rol: str = Form("tagger"),
):
    user = get_current_user(request)
    if not user or user["rol"] != "admin":
        return RedirectResponse("/", status_code=302)
    try:
        crear_usuario(username, password, rol)
    except Exception:
        pass
    return RedirectResponse("/admin/usuarios", status_code=302)


@app.post("/admin/usuarios/{usuario_id}/password")
def admin_cambiar_password(request: Request, usuario_id: int, password: str = Form(...)):
    user = get_current_user(request)
    if not user or user["rol"] != "admin":
        return RedirectResponse("/", status_code=302)
    cambiar_password(usuario_id, password)
    return RedirectResponse("/admin/usuarios", status_code=302)


@app.post("/admin/usuarios/{usuario_id}/eliminar")
def admin_eliminar_usuario(request: Request, usuario_id: int):
    user = get_current_user(request)
    if not user or user["rol"] != "admin":
        return RedirectResponse("/", status_code=302)
    target = obtener_usuario(usuario_id)
    if target and target["username"] != "admin":
        eliminar_usuario(usuario_id)
    return RedirectResponse("/admin/usuarios", status_code=302)


@app.get("/admin/tagger/{usuario_id}", response_class=HTMLResponse)
def admin_detalle_tagger(request: Request, usuario_id: int):
    user = get_current_user(request)
    if not user or user["rol"] != "admin":
        return RedirectResponse("/", status_code=302)
    detalle = obtener_detalle_tagger(usuario_id)
    return templates.TemplateResponse(request, "admin_tagger.html", {
        "user": user,
        "detalle": detalle,
        "formato_label": formato_label,
    })


@app.get("/admin/metricas", response_class=HTMLResponse)
def admin_metricas(request: Request):
    user = get_current_user(request)
    if not user or user["rol"] != "admin":
        return RedirectResponse("/", status_code=302)
    metricas = obtener_metricas_por_clase()
    return templates.TemplateResponse(request, "admin_metricas.html", {
        "user": user,
        "metricas": metricas,
        "formato_label": formato_label,
    })


@app.get("/api/subtemas/{tema}")
def api_subtemas(tema: str):
    return SUBTHEME_MAP.get(tema, [])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
