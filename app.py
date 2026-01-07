"""
Vehiculos - Aplicacion Flask
============================
Control de vehiculos para auditoria institucional.
"""

import logging
import sys
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List

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
            (ente.get("siglas") or "").upper(),
            (ente.get("nombre") or "").upper(),
        }
        if claves & permitidos:
            filtrados.append(ente)
    return filtrados


def _filtrar_vehiculos(items: List[dict]) -> List[dict]:
    return [item for item in items if item.get("categoria") == "VEHICULO"]


def _build_dashboard_context(app: Flask, db_manager: DatabaseManager, usuario_id: int) -> dict:
    items = _filtrar_vehiculos(db_manager.listar_items())
    vehiculos = db_manager.listar_vehiculos()
    responsables = db_manager.listar_responsables()
    auditores = db_manager.listar_auditores()
    notificaciones = db_manager.listar_notificaciones()
    movimientos = db_manager.listar_movimientos(usuario_id=usuario_id)
    entes = _filtrar_entes(db_manager.listar_entes(), session.get("entes", []))
    alertas = db_manager.movimientos_con_alerta(
        movimientos,
        app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
    )
    return {
        "items": items,
        "vehiculos": vehiculos,
        "responsables": responsables,
        "auditores": auditores,
        "notificaciones": notificaciones,
        "entes": entes,
        "movimientos": alertas,
        "today": date.today().isoformat(),
    }


def _build_monitor_context(app: Flask, db_manager: DatabaseManager) -> dict:
    items = _filtrar_vehiculos(db_manager.listar_items())
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

            session.update({
                "usuario_id": user["id"],
                "usuario": user["usuario"],
                "nombre": user["nombre"],
                "rol": user["rol"],
                "entes": user["entes"],
                "autenticado": True,
            })

            if user["rol"] == "gestor":
                destino = "gestor"
            elif user["rol"] == "monitor":
                destino = "monitoreo"
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
        if session.get("rol") == "gestor":
            return redirect(url_for("gestor"))
        if session.get("rol") == "monitor":
            return redirect(url_for("monitoreo"))

        context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
        return render_template("dashboard.html", usuario=session.get("nombre"), **context)

    @app.route("/monitoreo")
    def monitoreo():
        if session.get("rol") == "gestor":
            return redirect(url_for("gestor"))
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))
        context = _build_monitor_context(app, db_manager)
        return render_template("monitor.html", usuario=session.get("nombre"), **context)

    @app.route("/solicitar", methods=["POST"])
    def solicitar():
        if not session.get("autenticado"):
            return redirect(url_for("login"))
        if session.get("rol") != "usuario":
            return redirect(url_for("dashboard"))

        item_id = request.form.get("item_id")
        ente = request.form.get("ente")
        cantidad = request.form.get("cantidad") or "1"
        resguardante = request.form.get("resguardante_nombre")
        vehiculo_id = request.form.get("vehiculo_id")
        responsable_id = request.form.get("responsable_id")
        no_pasajeros = request.form.get("no_pasajeros")
        auditores = request.form.getlist("auditores_ids")
        ruta_destino = request.form.get("ruta_destino")
        motivo = request.form.get("motivo_salida")
        tipo_notificacion_id = request.form.get("tipo_notificacion_id")
        observaciones = request.form.get("observaciones")
        fecha = request.form.get("fecha_solicitud")

        ok, data = db_manager.crear_movimiento(
            int(item_id),
            session["usuario_id"],
            ente,
            int(cantidad),
            resguardante or "",
            None,
            observaciones,
            resguardante or "",
            int(vehiculo_id) if vehiculo_id else None,
            int(responsable_id) if responsable_id else None,
            int(no_pasajeros) if no_pasajeros else None,
            [int(auditor_id) for auditor_id in auditores if auditor_id],
            ruta_destino or "",
            motivo or "",
            int(tipo_notificacion_id) if tipo_notificacion_id else None,
            fecha_solicitud=fecha,
        )

        if not ok:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error=data.get("mensaje"),
                **context,
            )

        return redirect(url_for("reporte_movimiento", mov_id=data["movimiento_id"]))

    @app.route("/gestor")
    def gestor():
        if session.get("rol") != "gestor":
            return redirect(url_for("dashboard"))
        items = _filtrar_vehiculos(db_manager.listar_items(activos=False))
        movimientos = db_manager.listar_movimientos()
        alertas = db_manager.movimientos_con_alerta(
            movimientos,
            app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
        )
        return render_template(
            "gestor.html",
            items=items,
            movimientos=alertas,
            usuario=session.get("nombre"),
        )

    @app.route("/inventario/nuevo", methods=["POST"])
    def inventario_nuevo():
        if session.get("rol") != "gestor":
            return redirect(url_for("dashboard"))

        sigla = request.form.get("sigla")
        nombre = request.form.get("nombre")
        categoria = request.form.get("categoria", "VEHICULO")
        no_inventario = request.form.get("no_inventario")
        descripcion = request.form.get("descripcion")
        stock_total = request.form.get("stock_total", 0)

        ok, mensaje = db_manager.crear_item(
            sigla,
            nombre,
            categoria,
            no_inventario,
            descripcion,
            int(stock_total),
        )
        if not ok:
            items = _filtrar_vehiculos(db_manager.listar_items(activos=False))
            movimientos = db_manager.listar_movimientos()
            alertas = db_manager.movimientos_con_alerta(
                movimientos,
                app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
            )
            return render_template(
                "gestor.html",
                items=items,
                movimientos=alertas,
                usuario=session.get("nombre"),
                error=mensaje,
            )

        return redirect(url_for("gestor"))

    @app.route("/movimientos/<int:mov_id>/entregar", methods=["POST"])
    def movimientos_entregar(mov_id: int):
        if session.get("rol") != "gestor":
            return redirect(url_for("dashboard"))
        db_manager.marcar_entregado(mov_id, session.get("usuario_id"))
        return redirect(url_for("gestor"))

    @app.route("/movimientos/<int:mov_id>/devolver", methods=["POST"])
    def movimientos_devolver(mov_id: int):
        if session.get("rol") != "gestor":
            return redirect(url_for("dashboard"))
        db_manager.marcar_devuelto(mov_id, session.get("usuario_id"))
        return redirect(url_for("gestor"))

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
        responsable = (movimiento.get("responsable_vehiculo") or "").strip().lower()
        usuario_nombre = (session.get("nombre") or "").strip().lower()
        can_print = bool(responsable and usuario_nombre and responsable == usuario_nombre)
        fecha_larga = _fecha_larga_es(movimiento.get("fecha_solicitud"))
        return render_template(
            "reporte.html",
            movimiento=movimiento,
            fecha_larga=fecha_larga,
            can_print=can_print,
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


app = create_app()


if __name__ == "__main__":
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
