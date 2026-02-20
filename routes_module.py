# routes_module.py
import os
import re
import csv
import io
import sqlite3
import datetime
from urllib.parse import quote_plus

import pdfplumber
from flask import (
    Blueprint, request, render_template, jsonify,
    redirect, url_for, session, abort, send_file
)
from werkzeug.utils import secure_filename

# -----------------------
# Helpers de auth (usa session del app principal)
# -----------------------
def login_required(fn):
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    return wrapper

def module_required(module_id: str):
    def deco(fn):
        def wrapper(*args, **kwargs):
            if not session.get("logged_in"):
                return redirect(url_for("login"))
            if session.get("role") == "admin":
                return fn(*args, **kwargs)
            allowed = session.get("modules_list", []) or []
            if module_id not in allowed:
                abort(403)
            return fn(*args, **kwargs)
        wrapper.__name__ = fn.__name__
        return wrapper
    return deco

# -----------------------
# Blueprint
# -----------------------
rutas_bp = Blueprint("rutas", __name__, url_prefix="/rutas")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
def p(*parts): return os.path.join(BASE_DIR, *parts)

UPLOAD_FOLDER = p("uploads_rutas")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

DB_ROUTES = p("routes.db")

# -----------------------
# DB
# -----------------------
def db():
    conn = sqlite3.connect(DB_ROUTES)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS rutas_runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            fecha TEXT NOT NULL,
            proyecto TEXT DEFAULT '',
            origen_general TEXT DEFAULT '',
            destino_general TEXT DEFAULT '',
            pdf_filename TEXT DEFAULT ''
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS rutas_lines(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            tipo TEXT DEFAULT '',
            colaborador TEXT DEFAULT '',
            conductor TEXT DEFAULT '',
            vehiculo TEXT DEFAULT '',
            remolque TEXT DEFAULT '',
            origen TEXT DEFAULT '',
            destino TEXT DEFAULT '',
            salida TEXT DEFAULT '',
            llegada TEXT DEFAULT '',
            notas TEXT DEFAULT '',
            maps_url TEXT DEFAULT '',
            FOREIGN KEY(run_id) REFERENCES rutas_runs(id) ON DELETE CASCADE
        )
        """)
        conn.commit()

def cleanup_keep_last_31_days():
    # Borra runs de más de 31 días (y sus lines por cascade si activas PRAGMA foreign_keys)
    cutoff = (datetime.date.today() - datetime.timedelta(days=31)).isoformat()
    with db() as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        old = conn.execute("SELECT id FROM rutas_runs WHERE fecha < ?", (cutoff,)).fetchall()
        for r in old:
            conn.execute("DELETE FROM rutas_lines WHERE run_id=?", (r["id"],))
            conn.execute("DELETE FROM rutas_runs WHERE id=?", (r["id"],))
        conn.commit()

# -----------------------
# Parser PDF ECI (robusto)
# - En estos PDFs muchas veces la tabla se “aplana” en 1 celda
# - Vamos a:
#   1) extraer texto por página (rápido)
#   2) buscar líneas que empiezan por ID_Envio (números) + hora + fecha
#   3) sacar origen/destino de forma razonable
# -----------------------
RX_DATE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
RX_TIME = re.compile(r"\b(\d{2}:\d{2}:\d{2})\b")

def guess_fecha_inicio_from_text(text: str) -> str | None:
    # Si aparece "FechaInicioJornada 16/02/2026" o similar, usa esa
    m = re.search(r"FechaInicioJornada\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})", text, re.IGNORECASE)
    if m:
        d = m.group(1)
        return to_iso_date(d)
    # fallback: primera fecha que aparezca
    m2 = RX_DATE.search(text)
    if m2:
        return to_iso_date(m2.group(1))
    return None

def to_iso_date(ddmmyyyy: str) -> str:
    d, m, y = ddmmyyyy.split("/")
    return f"{y}-{m}-{d}"

def normalize_spaces(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def make_maps_url(destino: str) -> str:
    q = quote_plus((destino or "").strip())
    return f"https://www.google.com/maps/search/?api=1&query={q}" if q else ""

def parse_eci_pdf(pdf_path: str) -> dict:
    """
    Devuelve:
      {
        "fecha": "YYYY-MM-DD",
        "rows": [ {colaborador, tipo, origen, destino, salida, llegada, notas, maps_url}, ... ]
      }
    """
    all_lines = []
    first_text = ""

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            # Modo “rápido” para evitar tardanza
            try:
                txt = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
            except Exception:
                txt = ""
            if not txt.strip():
                try:
                    txt = page.extract_text_simple() or ""
                except Exception:
                    txt = ""
            if i == 0:
                first_text = txt or ""
            for ln in (txt.splitlines() if txt else []):
                ln = normalize_spaces(ln)
                if ln:
                    all_lines.append(ln)

    full_text = "\n".join(all_lines) if all_lines else (first_text or "")
    fecha = guess_fecha_inicio_from_text(full_text) or datetime.date.today().isoformat()

    # En muchos casos, cada “registro” empieza con un ID largo numérico
    # Ejemplo: 440160220261103 ...
    # Vamos a capturar bloques que empiecen por dígitos y contengan una hora y una fecha
    candidates = []
    for ln in all_lines:
        if re.match(r"^\d{6,}", ln) and RX_TIME.search(ln) and RX_DATE.search(ln):
            candidates.append(ln)

    # Si el PDF viene “aplastado”, puede que estén muchas rutas en una sola línea
    # ⇒ lo partimos por ocurrencias de ID numérico
    expanded = []
    for c in candidates:
        parts = re.split(r"(?=(?:\d{6,}\s))", c)
        for p0 in parts:
            p0 = normalize_spaces(p0)
            if re.match(r"^\d{6,}", p0) and RX_TIME.search(p0) and RX_DATE.search(p0):
                expanded.append(p0)

    # Parser flexible:
    # ID + origen_code + origen_text + ... + hora_posicion + ... + fecha_desc + hora_desc + obs
    rows = []
    for line in expanded:
        # Saca campos “seguros”
        idm = re.match(r"^(\d{6,})\s+(.*)$", line)
        if not idm:
            continue
        _id = idm.group(1)
        rest = idm.group(2)

        # origen suele empezar con código 3 dígitos (050, 083...)
        m_or = re.match(r"^(\d{3})\s+(.*)$", rest)
        origen_code = ""
        rest2 = rest
        if m_or:
            origen_code = m_or.group(1)
            rest2 = m_or.group(2)

        # fecha y hora descarga (las últimas)
        m_date = list(RX_DATE.finditer(rest2))
        m_time = list(RX_TIME.finditer(rest2))
        fecha_desc = m_date[-1].group(1) if m_date else ""
        hora_desc = m_time[-1].group(1) if m_time else ""

        # hora posicion suele ser la primera hora
        hora_pos = m_time[0].group(1) if m_time else ""

        # “Observaciones” = lo que queda después de la última hora_desc
        notas = ""
        if hora_desc:
            idx = rest2.rfind(hora_desc)
            if idx != -1:
                notas = rest2[idx + len(hora_desc):].strip(" -")
        notas = normalize_spaces(notas)

        # Origen / Destino aproximados:
        # antes de hora_pos suele estar: ORIGEN + DESTINO + "Retorno/Sencillo/..."
        before_time = rest2.split(hora_pos, 1)[0].strip() if hora_pos and hora_pos in rest2 else rest2

        # Quita tokens típicos (Retorno, Sencillo, etc.) para intentar separar destino
        before_time_clean = re.sub(r"\b(Retorno|Sencillo\.?|Intermedia|Prov\d+)\b", "", before_time, flags=re.IGNORECASE)
        before_time_clean = normalize_spaces(before_time_clean)

        # Heurística: Origen = primeras 2-6 palabras (suele ser tienda/origen), destino = resto
        words = before_time_clean.split()
        origen_txt = " ".join(words[:6]).strip()
        destino_txt = " ".join(words[6:]).strip()

        if origen_code:
            origen_txt = f"{origen_code} {origen_txt}".strip()

        # Tipo: detecta “Retorno” / “Domingo” / etc. (si aparece)
        tipo = "PROPIOS"
        if re.search(r"\bRetorno\b", line, re.IGNORECASE):
            tipo = "RETORNO"
        if re.search(r"\bDomingo\b", line, re.IGNORECASE):
            tipo = "DOMINGO"

        maps_url = make_maps_url(destino_txt or origen_txt)

        rows.append({
            "id_envio": _id,
            "tipo": tipo,
            "colaborador": "",    # se rellena en pantalla
            "conductor": "",      # se rellena en pantalla
            "vehiculo": "",       # se rellena en pantalla
            "remolque": "",       # se rellena en pantalla
            "origen": origen_txt,
            "destino": destino_txt,
            "salida": hora_pos,
            "llegada": hora_desc,
            "notas": notas,
            "maps_url": maps_url
        })

    return {"fecha": fecha, "rows": rows}

# -----------------------
# PDF Orden de carga (simple)
# -----------------------
def build_orden_carga_pdf(payload: dict) -> bytes:
    # PDF mínimo y liviano sin depender de plantillas
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import mm

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    w, h = A4

    c.setFont("Helvetica-Bold", 14)
    c.drawString(18*mm, h-20*mm, "ORDEN DE CARGA")

    c.setFont("Helvetica", 10)
    y = h - 30*mm

    def line(label, value):
        nonlocal y
        c.setFont("Helvetica-Bold", 10)
        c.drawString(18*mm, y, f"{label}:")
        c.setFont("Helvetica", 10)
        c.drawString(55*mm, y, str(value or ""))
        y -= 6*mm

    line("Cliente", payload.get("cliente", "EL CORTE INGLÉS"))
    line("Proyecto", payload.get("proyecto", ""))
    line("Fecha", payload.get("fecha", ""))
    line("Origen", payload.get("origen_general", ""))
    line("Destino", payload.get("destino_general", ""))

    y -= 4*mm
    c.setFont("Helvetica-Bold", 10)
    c.drawString(18*mm, y, "Detalle de servicios")
    y -= 6*mm

    c.setFont("Helvetica", 9)
    for i, r in enumerate(payload.get("rows", [])[:25], start=1):
        txt = f"{i}. {r.get('origen','')} → {r.get('destino','')} | Salida: {r.get('salida','')} | Llegada: {r.get('llegada','')} | Notas: {r.get('notas','')}"
        c.drawString(18*mm, y, txt[:120])
        y -= 5*mm
        if y < 18*mm:
            c.showPage()
            y = h - 20*mm
            c.setFont("Helvetica", 9)

    c.showPage()
    c.save()
    return buf.getvalue()

# -----------------------
# Views
# -----------------------
@rutas_bp.before_app_request
def _init_once():
    # se ejecuta con tráfico; ligero
    init_db()

@rutas_bp.route("/", methods=["GET"])
@login_required
@module_required("rutas")
def rutas_home():
    # pantalla principal (ruta actual)
    return render_template("rutas.html")

@rutas_bp.route("/upload_pdf", methods=["POST"])
@login_required
@module_required("rutas")
def rutas_upload_pdf():
    f = request.files.get("pdf")
    if not f:
        return jsonify({"error": "No se recibió PDF"}), 400

    filename = secure_filename(f.filename or "ruta.pdf")
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"

    path = os.path.join(UPLOAD_FOLDER, filename)
    f.save(path)

    parsed = parse_eci_pdf(path)
    return jsonify({"ok": True, "fecha": parsed["fecha"], "rows": parsed["rows"], "pdf_filename": filename})

@rutas_bp.route("/save", methods=["POST"])
@login_required
@module_required("rutas")
def rutas_save():
    payload = request.json or {}
    fecha = (payload.get("fecha") or datetime.date.today().isoformat()).strip()
    proyecto = (payload.get("proyecto") or "").strip()
    origen_general = (payload.get("origen_general") or "").strip()
    destino_general = (payload.get("destino_general") or "").strip()
    pdf_filename = (payload.get("pdf_filename") or "").strip()
    rows = payload.get("rows", []) or []

    cleanup_keep_last_31_days()

    with db() as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = conn.execute(
            "INSERT INTO rutas_runs(created_at,fecha,proyecto,origen_general,destino_general,pdf_filename) VALUES(?,?,?,?,?,?)",
            (now, fecha, proyecto, origen_general, destino_general, pdf_filename)
        )
        run_id = cur.lastrowid

        for r in rows:
            destino = (r.get("destino") or "").strip()
            maps_url = make_maps_url(destino)
            conn.execute("""
                INSERT INTO rutas_lines(run_id,tipo,colaborador,conductor,vehiculo,remolque,origen,destino,salida,llegada,notas,maps_url)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                run_id,
                (r.get("tipo") or "").strip(),
                (r.get("colaborador") or "").strip(),
                (r.get("conductor") or "").strip(),
                (r.get("vehiculo") or "").strip(),
                (r.get("remolque") or "").strip(),
                (r.get("origen") or "").strip(),
                destino,
                (r.get("salida") or "").strip(),
                (r.get("llegada") or "").strip(),
                (r.get("notas") or "").strip(),
                maps_url
            ))
        conn.commit()

    return jsonify({"ok": True, "run_id": run_id})

@rutas_bp.route("/historial", methods=["GET"])
@login_required
@module_required("rutas")
def rutas_historial():
    cleanup_keep_last_31_days()
    with db() as conn:
        runs = conn.execute("""
            SELECT id, created_at, fecha, proyecto, origen_general, destino_general, pdf_filename
            FROM rutas_runs
            ORDER BY fecha DESC, id DESC
            LIMIT 200
        """).fetchall()
    return render_template("rutas_historial.html", runs=[dict(r) for r in runs])

@rutas_bp.route("/historial/<int:run_id>/json", methods=["GET"])
@login_required
@module_required("rutas")
def rutas_historial_run_json(run_id: int):
    with db() as conn:
        run = conn.execute("SELECT * FROM rutas_runs WHERE id=?", (run_id,)).fetchone()
        if not run:
            return jsonify({"error": "No existe"}), 404
        lines = conn.execute("SELECT * FROM rutas_lines WHERE run_id=? ORDER BY id ASC", (run_id,)).fetchall()
    return jsonify({"run": dict(run), "lines": [dict(x) for x in lines]})

@rutas_bp.route("/historial/export.csv", methods=["GET"])
@login_required
@module_required("rutas")
def rutas_historial_export_csv():
    cleanup_keep_last_31_days()
    out = io.StringIO()
    w = csv.writer(out, delimiter=";")
    w.writerow(["run_id","fecha","proyecto","tipo","colaborador","conductor","vehiculo","remolque","origen","destino","salida","llegada","notas","maps_url"])
    with db() as conn:
        rows = conn.execute("""
            SELECT r.id as run_id, r.fecha, r.proyecto,
                   l.tipo, l.colaborador, l.conductor, l.vehiculo, l.remolque,
                   l.origen, l.destino, l.salida, l.llegada, l.notas, l.maps_url
            FROM rutas_runs r
            JOIN rutas_lines l ON l.run_id = r.id
            WHERE r.fecha >= ?
            ORDER BY r.fecha DESC, r.id DESC, l.id ASC
        """, ((datetime.date.today() - datetime.timedelta(days=31)).isoformat(),)).fetchall()
        for r in rows:
            w.writerow([r[k] for k in r.keys()])

    data = out.getvalue().encode("utf-8")
    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name="historial_rutas_ultimos_31_dias.csv"
    )

@rutas_bp.route("/orden_carga.pdf", methods=["POST"])
@login_required
@module_required("rutas")
def rutas_orden_carga_pdf():
    payload = request.json or {}
    pdf_bytes = build_orden_carga_pdf(payload)
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name="orden_de_carga.pdf"
    )

@rutas_bp.route("/whatsapp_text", methods=["POST"])
@login_required
@module_required("rutas")
def rutas_whatsapp_text():
    """
    Devuelve el texto para WhatsApp con link de Maps al DESTINO.
    """
    payload = request.json or {}
    conductor = (payload.get("conductor") or "").strip()
    origen = (payload.get("origen") or "").strip()
    destino = (payload.get("destino") or "").strip()
    salida = (payload.get("salida") or "").strip()
    proyecto = (payload.get("proyecto") or "").strip()
    notas = (payload.get("notas") or "").strip()

    maps = make_maps_url(destino)

    text = (
        f"📍 *RUTA ASIGNADA*\n"
        f"👤 Conductor: {conductor}\n"
        f"🧾 Proyecto: {proyecto}\n"
        f"🚚 Origen: {origen}\n"
        f"🎯 Destino: {destino}\n"
        f"⏱️ Salida: {salida}\n"
    )
    if notas:
        text += f"📝 Notas: {notas}\n"
    if maps:
        text += f"🗺️ Maps destino: {maps}\n"

    return jsonify({"text": text})
