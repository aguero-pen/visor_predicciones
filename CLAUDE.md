# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Comandos clave

```bash
# Levantar todo (PostgreSQL + app + Cloudflare Tunnel)
docker compose up -d

# Reconstruir y redesplegar solo la app (lo más común tras cambios de código)
docker compose up -d --build app

# Ver logs de la app
docker compose logs app --tail 20

# Reiniciar solo el túnel si se cae
docker compose up -d tunnel

# Verificar estado de contenedores
docker compose ps

# Acceder a psql
docker compose exec db psql -U visor visor_predicciones

# Desarrollo local (sin Docker)
source venv/bin/activate
uvicorn app:app --reload --port 8000
```

## Arquitectura

La app es un sistema de validación manual de predicciones de ML sobre intervenciones legislativas (~79k filas). Múltiples taggers clasifican las mismas intervenciones y se calcula consenso por mayoría.

### Stack
- **FastAPI** + **Jinja2** + **Pico CSS** (sin JavaScript frameworks)
- **PostgreSQL 16** en Docker con volumen persistente `pgdata`
- **Cloudflare Tunnel** expone `visor.automatiza.cc` sin abrir puertos
- **`.env`** (en `.gitignore`) contiene `DATABASE_URL`, `TUNNEL_TOKEN`, `SECRET_KEY`

### Archivos principales
- `database.py` — toda la capa de datos. Contiene `init_db()` que crea tablas, aplica migraciones y crea índices automáticamente al arrancar
- `app.py` — rutas FastAPI, autenticación por cookie firmada (`itsdangerous`), lógica de cola de revisión
- `templates/` — Jinja2 con `base.html` como layout base
- `static/style.css` — estilos custom sobre Pico CSS

### Base de datos
Tres tablas: `usuarios`, `intervenciones`, `validaciones`.

`intervenciones` tiene 68 columnas: metadatos (nombre_diputado, fecha, tipo_sesion, nsesion, etc.), 7 columnas de probabilidad de tema (`tema_1_*` ... `tema_7_*`), 49 columnas de probabilidad de subtema (`subtema_2_*` ... `subtema_99_*`), y 4 columnas pre-computadas para rendimiento: `tema_principal`, `subtema_principal`, `anio`, `mes`.

Las columnas pre-computadas se llenan automáticamente en `init_db()` si están vacías (función `_poblar_columnas_computadas`). **No usar CASE/GREATEST en queries de exploración — usar las columnas directas.**

Índices existentes: `idx_interv_tema`, `idx_interv_subtema`, `idx_interv_anio`, `idx_interv_diputado` (GIN trgm), `idx_interv_texto` (GIN trgm), `idx_interv_sesion`, `idx_interv_mes`. La extensión `pg_trgm` está habilitada — los ILIKE con `%palabra%` usan índice.

### Autenticación
Cookie `session` firmada con `URLSafeSerializer`. Roles: `admin` y `tagger`. El admin por defecto se crea en `init_db()` (usuario: `admin`, password: `admin`).

### Cola de revisión (`seleccionar_pendiente_balanceado`)
1. **Prioridad 1**: items ya validados por otros pero no por el usuario actual, balanceado por tema
2. **Prioridad 2**: items no validados por nadie, balanceado por tema

El caché `_temas_cache` se carga al startup con todos los temas en una query.

### Jerarquía de clasificación
7 temas principales → 49 subtemas. Definida en `MAIN_THEMES` y `SUBTHEME_MAP` en `app.py`, y duplicada en `SUBTHEME_MAP_DB` en `database.py` (para evitar import circular).

### Convenciones
- Commits: conventional commits en español, por archivo
- Sin Claude como co-autor en commits
- El CSV `predicciones.csv` se carga a PostgreSQL al startup si la tabla está vacía
- Nuevos CSVs se cargan manualmente con el script embebido en `cargar_csv_a_db` o ejecutando python directo en el contenedor
