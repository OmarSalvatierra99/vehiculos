"""
Vehiculos - Aplicacion Flask
============================
Control de vehiculos para auditoria institucional.
"""

import logging
import sys
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional, Tuple

from flask import (
    Flask,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.exceptions import HTTPException

from config import get_config
from scripts.utils import DatabaseManager


def create_app(config_name: str = None) -> Flask:
    """Factory para crear y configurar la aplicacion Flask."""
    app = Flask(__name__)
    config = get_config(config_name)
    app.config.from_object(config)

    _setup_logging(app)
    _setup_security_headers(app)
    _setup_error_handlers(app)

    db_manager = DatabaseManager(
        app.config["INVENTARIOS_DB"],
        app.config["CATALOGOS_DIR"],
    )

    _register_routes(app, db_manager)

    app.logger.info("Vehiculos iniciado - Entorno: %s", config_name or "default")
    return app


def _setup_logging(app: Flask) -> None:
    log_level_name = app.config.get("LOG_LEVEL", "INFO")
    log_level = getattr(logging, log_level_name.upper(), logging.INFO)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_file = app.config.get("LOG_FILE", "log/app.log")
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=10,
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    app.logger.setLevel(log_level)
    app.logger.addHandler(file_handler)
    app.logger.addHandler(console_handler)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


def _setup_security_headers(app: Flask) -> None:
    @app.after_request
    def add_security_headers(response):
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src 'self' https://fonts.gstatic.com; "
            "img-src 'self' data:; "
            "script-src 'self' 'unsafe-inline';"
        )

        if not app.config.get("DEBUG", False):
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        return response


def _setup_error_handlers(app: Flask) -> None:
    @app.errorhandler(HTTPException)
    def handle_http_exception(error: HTTPException):
        app.logger.warning("HTTPException: %s - %s", error.code, error.description)
        return jsonify({
            "error": error.name,
            "message": error.description,
            "status": error.code,
        }), error.code

    @app.errorhandler(Exception)
    def handle_unexpected_error(error: Exception):
        app.logger.error("Error inesperado: %s", str(error), exc_info=True)
        return jsonify({
            "error": "Error interno del servidor",
            "message": "Ocurrió un error inesperado procesando su solicitud",
        }), 500


def _filtrar_entes(entes: List[dict], permitidos: List[str]) -> List[dict]:
    if not permitidos or "TODOS" in permitidos:
        return entes
    permitidos = {e.upper().strip() for e in permitidos}
    filtrados = []
    for ente in entes:
        claves = {
            (ente.get("clave") or "").upper(),
            (ente.get("nombre") or "").upper(),
        }
        if claves & permitidos:
            filtrados.append(ente)
    return filtrados


def _filtrar_vehiculos(items: List[dict]) -> List[dict]:
    return [item for item in items if item.get("categoria") == "VEHICULO"]


def _normalizar_rol(rol: str) -> str:
    rol_txt = (rol or "").strip().lower()
    if rol_txt == "monitor":
        return "monitor"
    if rol_txt in {"gestor", "admin", "administrador"}:
        return "admin"
    if rol_txt in {"usuario", "user"}:
        return "user"
    return "user"


def _parse_responsable_ref(raw: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    if not raw:
        return None, None
    raw = raw.strip()
    if raw.startswith("auditor:"):
        _, id_txt = raw.split(":", 1)
        id_txt = id_txt.strip()
        if id_txt.isdigit():
            return "auditor", int(id_txt)
        return None, None
    if raw.isdigit():
        return "usuario", int(raw)
    return None, None


def _normalizar_fecha_solicitud(fecha_txt: Optional[str]) -> str:
    if not fecha_txt:
        return date.today().isoformat()
    try:
        return datetime.strptime(fecha_txt, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return date.today().isoformat()


def _fecha_iso_desde_texto(fecha_txt: Optional[str]) -> Optional[str]:
    if not fecha_txt:
        return None
    fecha_base = str(fecha_txt).strip()[:10]
    try:
        return datetime.strptime(fecha_base, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _filtrar_movimientos_hoy(movimientos: List[dict], hoy: Optional[date] = None) -> List[dict]:
    hoy_iso = (hoy or date.today()).isoformat()
    filtrados = []
    for mov in movimientos:
        if _fecha_iso_desde_texto(mov.get("fecha_solicitud")) == hoy_iso:
            filtrados.append(mov)
    return filtrados


def _limites_semana_laboral(base: Optional[date] = None, total: int = 5) -> Tuple[date, date]:
    hoy = base or date.today()
    inicio = hoy
    if inicio.weekday() >= 5:
        dias_hasta_lunes = (7 - inicio.weekday()) % 7
        inicio = inicio + timedelta(days=dias_hasta_lunes)

    fin = inicio
    dias_contados = 1
    while dias_contados < total:
        fin += timedelta(days=1)
        if fin.weekday() < 5:
            dias_contados += 1
    return inicio, fin


def _proximos_dias_habiles(base: Optional[date] = None, total: int = 5) -> List[str]:
    inicio, fin = _limites_semana_laboral(base, total)
    dias = []
    actual = inicio
    while actual <= fin:
        if actual.weekday() < 5:
            dias.append(actual.isoformat())
        actual += timedelta(days=1)
    return dias


def _fecha_laboral_valida(fecha_txt: str) -> bool:
    try:
        fecha = datetime.strptime(fecha_txt, "%Y-%m-%d").date()
    except ValueError:
        return False
    if fecha.weekday() >= 5:
        return False
    inicio, fin = _limites_semana_laboral()
    return inicio <= fecha <= fin


def _solicitudes_bloqueadas(usuario: Optional[str]) -> bool:
    return False


def _es_visor_omar(usuario: Optional[str]) -> bool:
    return (usuario or "").strip().lower() == "omar"


def _build_dashboard_context(
    app: Flask,
    db_manager: DatabaseManager,
    usuario_id: int,
    fecha_solicitud: Optional[str] = None,
) -> dict:
    fecha_txt = _normalizar_fecha_solicitud(fecha_solicitud)
    usuario_actual = (session.get("usuario") or "").strip().lower()
    if usuario_actual in {"mike", "ramos"}:
        omar_id = db_manager.obtener_usuario_id("omar")
        vehiculos = db_manager.listar_vehiculos(usuario_id=omar_id) if omar_id else []
    else:
        vehiculos = db_manager.listar_vehiculos(usuario_id=usuario_id)
    ocupados = db_manager.obtener_vehiculos_ocupados(fecha_txt)
    vehiculos = [vehiculo for vehiculo in vehiculos if vehiculo.get("id") not in ocupados]
    ocupados_auditores = db_manager.obtener_auditores_ocupados(fecha_txt)
    ocupados_responsables_usuario = db_manager.obtener_usuarios_responsables_ocupados(fecha_txt)
    auditores_ofs = [
        item for item in db_manager.listar_auditores()
        if item.get("id") not in ocupados_auditores
    ]
    auditores_ofs_por_id = {item.get("id"): item for item in auditores_ofs if item.get("id")}

    def _dedupe_por_id(items: List[dict]) -> List[dict]:
        seen = set()
        unique = []
        for item in items:
            item_id = item.get("id")
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            unique.append(item)
        return unique

    responsables = _dedupe_por_id([
        item for item in db_manager.listar_personal_resguardante(usuario_id)
        if item.get("id") not in ocupados_auditores
    ])
    auditores = _dedupe_por_id([
        item for item in db_manager.listar_auditores_por_usuario(usuario_id)
        if item.get("id") not in ocupados_auditores
    ])

    # Asegura que el resguardante (usuario actual) pueda aparecer tambien como auditor,
    # cuando exista en el catalogo de auditores y no este ocupado.
    nombre_usuario = (session.get("nombre") or "").strip().lower()
    auditor_resguardante_id = None
    for auditor in auditores_ofs:
        if (auditor.get("nombre") or "").strip().lower() == nombre_usuario:
            auditor_resguardante_id = auditor.get("id")
            break
    if auditor_resguardante_id and auditor_resguardante_id in auditores_ofs_por_id:
        auditor_resguardante = auditores_ofs_por_id[auditor_resguardante_id]
        if all(item.get("id") != auditor_resguardante_id for item in auditores):
            auditores.append(auditor_resguardante)
        if all(item.get("id") != auditor_resguardante_id for item in responsables):
            responsables.append(auditor_resguardante)

    if not auditores:
        auditores = auditores_ofs
    entes = db_manager.listar_entes()
    vehiculos_prestables = db_manager.listar_vehiculos_prestables(usuario_id)
    movimientos = _filtrar_movimientos_hoy(
        db_manager.listar_movimientos(usuario_id=usuario_id)
    )
    alertas = db_manager.movimientos_con_alerta(
        movimientos,
        app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
    )
    return {
        "movimientos": alertas,
        "mis_movimientos": movimientos,
        "vehiculos": vehiculos,
        "responsables": responsables,
        "auditores": auditores,
        "auditores_ofs": auditores_ofs,
        "entes": entes,
        "vehiculos_prestables": vehiculos_prestables,
        "usuario_id": usuario_id,
        "resguardante_disponible": usuario_id not in ocupados_responsables_usuario,
        "auditor_resguardante_id": auditor_resguardante_id,
        "today": date.today().isoformat(),
        "fecha_min": _limites_semana_laboral()[0].isoformat(),
        "fecha_max": _limites_semana_laboral()[1].isoformat(),
        "fechas_habiles": _proximos_dias_habiles(),
        "fecha_solicitud": fecha_txt,
    }


def _build_admin_context(app: Flask, db_manager: DatabaseManager, rol: Optional[str] = None) -> dict:
    movimientos = db_manager.listar_movimientos()
    if rol == "monitor":
        movimientos = _filtrar_movimientos_hoy(movimientos)
    alertas = db_manager.movimientos_con_alerta(
        movimientos,
        app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
    )
    if rol == "monitor":
        vehiculos = db_manager.listar_vehiculos_con_propietarios()
    else:
        vehiculos = db_manager.listar_vehiculos()
    responsables = db_manager.listar_responsables()
    total_stock, total_disponible = db_manager.contar_vehiculos_disponibles()
    en_uso = sum(
        1
        for mov in alertas
        if mov.data.get("fecha_entrega") and not mov.data.get("devuelto")
    )
    pendientes = sum(1 for mov in alertas if not mov.data.get("fecha_entrega"))
    total_alertas = sum(1 for mov in alertas if mov.alerta)
    return {
        "movimientos": alertas,
        "vehiculos": vehiculos,
        "responsables": responsables,
        "total_stock": total_stock,
        "total_disponible": total_disponible,
        "total_vehiculos": len(vehiculos),
        "movimientos_en_uso": en_uso,
        "movimientos_pendientes": pendientes,
        "movimientos_alerta": total_alertas,
        "today": date.today().isoformat(),
    }


def _build_monitor_context(app: Flask, db_manager: DatabaseManager) -> dict:
    items = db_manager.listar_vehiculos_disponibles()
    movimientos = db_manager.listar_movimientos()
    alertas = db_manager.movimientos_con_alerta(
        movimientos,
        app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
    )
    return {
        "items": items,
        "movimientos": alertas,
    }


def _register_routes(app: Flask, db_manager: DatabaseManager) -> None:
    @app.before_request
    def verificar_autenticacion():
        libres = {"login", "login_alias", "static", "health_check"}
        if request.endpoint not in libres and not session.get("autenticado"):
            return redirect(url_for("login"))

    @app.route("/", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            usuario = request.form.get("usuario", "").strip()
            clave = request.form.get("clave", "").strip()
            user = db_manager.get_usuario(usuario, clave)
            if not user:
                return render_template("login.html", error="Credenciales inválidas")

            rol = _normalizar_rol(user["rol"])
            session.update({
                "usuario_id": user["id"],
                "usuario": user["usuario"],
                "nombre": user["nombre"],
                "rol": rol,
                "entes": user["entes"],
                "autenticado": True,
            })

            if rol == "admin":
                destino = "admin"
            elif rol == "monitor":
                destino = "admin"
            else:
                destino = "dashboard"
            return redirect(url_for(destino))
        return render_template("login.html")

    @app.route("/login")
    def login_alias():
        return redirect(url_for("login"))

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.route("/dashboard")
    def dashboard():
        if session.get("rol") in {"admin", "monitor"}:
            return redirect(url_for("admin"))

        fecha = request.args.get("fecha")
        context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
        context["solicitudes_bloqueadas"] = _solicitudes_bloqueadas(session.get("usuario"))
        if _es_visor_omar(session.get("usuario")):
            context["movimientos_usuarios_observados"] = _filtrar_movimientos_hoy(
                db_manager.listar_movimientos_por_usuarios(["ramos", "mike"])
            )
        return render_template(
            "dashboard.html",
            usuario=session.get("nombre"),
            tipo_solicitud="normal",
            **context,
        )

    @app.route("/admin")
    def admin():
        if session.get("rol") not in {"admin", "monitor"}:
            return redirect(url_for("dashboard"))
        context = _build_admin_context(app, db_manager, session.get("rol"))
        return render_template(
            "admin.html",
            usuario=session.get("nombre"),
            rol=session.get("rol"),
            **context,
        )

    @app.route("/monitoreo")
    def monitoreo():
        return redirect(url_for("admin"))

    @app.route("/solicitar", methods=["POST"])
    def solicitar():
        if not session.get("autenticado"):
            return redirect(url_for("login"))
        if session.get("rol") != "user":
            return redirect(url_for("dashboard"))

        if _solicitudes_bloqueadas(session.get("usuario")):
            fecha = request.form.get("fecha_solicitud", "").strip() or date.today().isoformat()
            tipo_solicitud = request.form.get("tipo_solicitud", "").strip().lower()
            if not tipo_solicitud:
                tipo_solicitud = "prestamo" if request.form.get("prestamo_vehiculo") else "normal"
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            context["solicitudes_bloqueadas"] = True
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="Las solicitudes de vehiculo se reciben hasta las 19:00 h.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        tipo_solicitud = request.form.get("tipo_solicitud", "").strip().lower()
        if not tipo_solicitud:
            tipo_solicitud = "prestamo" if request.form.get("prestamo_vehiculo") else "normal"
        es_prestamo = tipo_solicitud == "prestamo"

        cantidad = request.form.get("cantidad") or "1"
        vehiculo_raw = request.form.get("vehiculo_id", "")
        responsable_raw = request.form.get("responsable_usuario_id")
        no_pasajeros_txt = request.form.get("no_pasajeros", "").strip()
        pasajeros_ids = [pid for pid in request.form.getlist("pasajeros_ids") if pid]
        ruta_destinos = [clave for clave in request.form.getlist("ruta_destinos") if clave]
        motivo = request.form.get("motivo_salida", "").strip()
        notas = request.form.get("notas")
        fecha = request.form.get("fecha_solicitud", "").strip()
        if not fecha:
            fecha = date.today().isoformat()

        if es_prestamo and not vehiculo_raw:
            vehiculo_raw = request.form.get("prestamo_vehiculo", "")
            responsable_raw = request.form.get("prestamo_responsable_usuario_id") or responsable_raw
            no_pasajeros_txt = request.form.get("prestamo_no_pasajeros", "").strip() or no_pasajeros_txt
            pasajeros_ids = [pid for pid in request.form.getlist("prestamo_pasajeros_ids") if pid] or pasajeros_ids
            ruta_destinos = [clave for clave in request.form.getlist("prestamo_ruta_destinos") if clave] or ruta_destinos
            motivo = request.form.get("prestamo_motivo_salida", "").strip() or motivo
            notas = request.form.get("prestamo_notas") or notas

        vehiculo_id = None
        propietario_id = None
        if ":" in vehiculo_raw:
            vehiculo_txt, propietario_txt = vehiculo_raw.split(":", 1)
        else:
            vehiculo_txt, propietario_txt = vehiculo_raw, ""
        if vehiculo_txt and vehiculo_txt.isdigit():
            vehiculo_id = int(vehiculo_txt)
        if propietario_txt and propietario_txt.isdigit():
            propietario_id = int(propietario_txt)

        if not vehiculo_id:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="Falta seleccionar la unidad.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )
        if es_prestamo and (not propietario_id or propietario_id == session.get("usuario_id")):
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="Seleccione una unidad asignada a otro usuario para el prestamo.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        responsable_tipo, responsable_id = _parse_responsable_ref(responsable_raw)
        if not responsable_tipo or responsable_id is None:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="Falta seleccionar al responsable del vehiculo.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        if not no_pasajeros_txt.isdigit():
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El numero de pasajeros debe ser numerico.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )
        no_pasajeros = int(no_pasajeros_txt)
        if no_pasajeros < 0:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El numero de pasajeros no puede ser menor a cero.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )
        if no_pasajeros > 4:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El numero de pasajeros no puede exceder 4 (maximo 5 ocupantes incluyendo al conductor).",
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        if len(pasajeros_ids) != no_pasajeros:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El numero de pasajeros debe coincidir con los nombres seleccionados.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )
        if any(not pid.isdigit() for pid in pasajeros_ids):
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="La lista de pasajeros no es valida.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        if not ruta_destinos:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="Falta seleccionar la ruta destino.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        motivos_validos = {
            "Notificación de Oficio",
            "Revisión de Auditoría",
            "Entrega de Recepción",
            "Acta de Cierre",
            "Compulsas",
            "Inspección Física",
        }
        if motivo not in motivos_validos:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El motivo de salida no es valido.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        if not es_prestamo:
            try:
                datetime.strptime(fecha, "%Y-%m-%d")
            except ValueError:
                context = _build_dashboard_context(
                    app,
                    db_manager,
                    session.get("usuario_id"),
                    _normalizar_fecha_solicitud(fecha),
                )
                return render_template(
                    "dashboard.html",
                    usuario=session.get("nombre"),
                    error="La fecha de solicitud no es valida.",
                    tipo_solicitud=tipo_solicitud,
                    **context,
                )
            if not _fecha_laboral_valida(fecha):
                context = _build_dashboard_context(
                    app,
                    db_manager,
                    session.get("usuario_id"),
                    _normalizar_fecha_solicitud(fecha),
                )
                return render_template(
                    "dashboard.html",
                    usuario=session.get("nombre"),
                    error="La fecha de salida debe estar dentro de los dias habiles permitidos.",
                    tipo_solicitud=tipo_solicitud,
                    **context,
                )

        if es_prestamo:
            fechas_prestamo = [fecha] if fecha else []
            ok, mensaje = db_manager.solicitar_prestamo(
                session.get("usuario_id"),
                int(propietario_id),
                int(vehiculo_id),
                responsable_id,
                responsable_tipo,
                int(no_pasajeros),
                [int(pid) for pid in pasajeros_ids],
                fechas_prestamo,
                ruta_destinos,
                motivo,
                notas,
            )
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                ok=mensaje if ok else None,
                error=None if ok else mensaje,
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        vehiculos = db_manager.listar_vehiculos(usuario_id=session.get("usuario_id"))
        vehiculo = next(
            (v for v in vehiculos if str(v.get("id")) == str(vehiculo_id)),
            None,
        )
        if not vehiculo:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="La unidad seleccionada no es valida.",
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        ok, data = db_manager.crear_movimiento(
            session["usuario_id"],
            ruta_destinos[0],
            int(cantidad),
            "",
            None,
            notas,
            None,
            int(vehiculo_id),
            responsable_id,
            responsable_tipo,
            int(no_pasajeros),
            [int(pid) for pid in pasajeros_ids],
            ruta_destinos,
            motivo,
            fecha_solicitud=fecha,
        )

        if not ok:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error=data.get("mensaje") if isinstance(data, dict) else data,
                tipo_solicitud=tipo_solicitud,
                **context,
            )

        return redirect(url_for("reporte_movimiento", mov_id=data["movimiento_id"]))

    @app.route("/prestamos/solicitar", methods=["POST"])
    def solicitar_prestamo():
        return solicitar()

    @app.route("/gestor")
    def gestor():
        return redirect(url_for("admin"))

    @app.route("/inventario/nuevo", methods=["POST"])
    def inventario_nuevo():
        if session.get("rol") != "admin":
            return redirect(url_for("dashboard"))

        placa = request.form.get("placa")
        marca = request.form.get("marca")
        modelo = request.form.get("modelo")

        ok, mensaje = db_manager.crear_vehiculo(
            placa,
            marca,
            modelo,
        )
        if not ok:
            movimientos = db_manager.listar_movimientos()
            alertas = db_manager.movimientos_con_alerta(
                movimientos,
                app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
            )
            return render_template(
                "admin.html",
                **_build_admin_context(app, db_manager, session.get("rol")),
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
            )

        return redirect(url_for("admin"))

    @app.route("/movimientos/<int:mov_id>/entregar", methods=["POST"])
    def movimientos_entregar(mov_id: int):
        if session.get("rol") not in {"admin", "monitor"}:
            return redirect(url_for("dashboard"))
        ok, mensaje = db_manager.marcar_entregado(mov_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol")),
            )
        return redirect(url_for("admin"))

    @app.route("/movimientos/<int:mov_id>/rechazar", methods=["POST"])
    def movimientos_rechazar(mov_id: int):
        if session.get("rol") not in {"admin", "monitor"}:
            return redirect(url_for("dashboard"))
        ok, mensaje = db_manager.marcar_rechazado(mov_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol")),
            )
        return redirect(url_for("admin"))

    @app.route("/prestamos/<int:prestamo_id>/validar", methods=["POST"])
    def prestamos_validar(prestamo_id: int):
        if session.get("rol") not in {"admin", "monitor"}:
            return redirect(url_for("dashboard"))
        ok, mensaje = db_manager.marcar_prestamo_validado(prestamo_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol")),
            )
        return redirect(url_for("admin"))

    @app.route("/prestamos/<int:prestamo_id>/rechazar", methods=["POST"])
    def prestamos_rechazar(prestamo_id: int):
        if session.get("rol") not in {"admin", "monitor"}:
            return redirect(url_for("dashboard"))
        ok, mensaje = db_manager.marcar_prestamo_rechazado(prestamo_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol")),
            )
        return redirect(url_for("admin"))

    @app.route("/movimientos/<int:mov_id>/devolver", methods=["POST"])
    def movimientos_devolver(mov_id: int):
        if session.get("rol") != "admin":
            return redirect(url_for("dashboard"))
        ok, mensaje = db_manager.marcar_devuelto(mov_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol")),
            )
        return redirect(url_for("admin"))

    @app.route("/health")
    def health_check():
        return jsonify({
            "status": "healthy",
            "service": "Vehiculos",
        }), 200

    @app.route("/reporte/<int:mov_id>")
    def reporte_movimiento(mov_id: int):
        if not session.get("autenticado"):
            return redirect(url_for("login"))
        movimiento = db_manager.obtener_movimiento(mov_id)
        if not movimiento:
            return redirect(url_for("dashboard"))
        can_print = session.get("rol") == "admin"
        fecha_larga = _fecha_larga_es(movimiento.get("fecha_solicitud"))
        return render_template(
            "reporte.html",
            movimiento=movimiento,
            fecha_larga=fecha_larga,
            can_print=can_print,
        )

    @app.route("/reporte-diario")
    def reporte_diario():
        if not session.get("autenticado"):
            return redirect(url_for("login"))
        if session.get("rol") != "monitor":
            return redirect(url_for("admin"))
        fecha = _normalizar_fecha_solicitud(request.args.get("fecha"))
        movimientos = db_manager.listar_movimientos_entregados(
            session.get("usuario_id"),
            fecha,
        )
        fecha_larga = _fecha_larga_es(fecha)
        return render_template(
            "reporte_diario.html",
            movimientos=movimientos,
            fecha_larga=fecha_larga,
            fecha=fecha,
            can_print=True,
        )


def _fecha_larga_es(fecha_iso: str) -> str:
    if not fecha_iso:
        return ""
    try:
        fecha = datetime.strptime(fecha_iso, "%Y-%m-%d").date()
    except ValueError:
        return fecha_iso
    dias = [
        "Lunes",
        "Martes",
        "Miercoles",
        "Jueves",
        "Viernes",
        "Sabado",
        "Domingo",
    ]
    meses = [
        "Enero",
        "Febrero",
        "Marzo",
        "Abril",
        "Mayo",
        "Junio",
        "Julio",
        "Agosto",
        "Septiembre",
        "Octubre",
        "Noviembre",
        "Diciembre",
    ]
    return f"{dias[fecha.weekday()]} {fecha.day:02d} de {meses[fecha.month - 1]} del {fecha.year}"


if __name__ == "__main__":
    app = create_app()
    host = app.config.get("HOST", "0.0.0.0")
    port = app.config.get("PORT", 5010)
    debug = app.config.get("DEBUG", False)
    env = app.config.get("ENV", "development")

    print(f"""
    ╔═══════════════════════════════════════════════════════════════╗
    ║                   Sistema de Vehiculos OFS                   ║
    ╚═══════════════════════════════════════════════════════════════╝

    🚀 Servidor iniciando...
    📍 Host: {host}
    🔌 Puerto: {port}
    🌍 Entorno: {env}
    🔧 Debug: {debug}

    ═══════════════════════════════════════════════════════════════
    """)

    try:
        app.run(host=host, port=port, debug=debug)
    except KeyboardInterrupt:
        print("\n\n👋 Servidor detenido por el usuario\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error iniciando servidor: {str(e)}\n")
        sys.exit(1)
