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


def _build_dashboard_context(app: Flask, db_manager: DatabaseManager, usuario_id: int) -> dict:
    items = _filtrar_vehiculos(db_manager.listar_items())
    item_siglas = {(item.get("sigla") or "").upper() for item in items}
    vehiculos = db_manager.listar_vehiculos(usuario_id=usuario_id)
    if item_siglas:
        vehiculos = [
            vehiculo
            for vehiculo in vehiculos
            if (vehiculo.get("placa") or "").upper() in item_siglas
        ]
    usuarios = db_manager.listar_usuarios(resguardante_id=usuario_id)
    entes = db_manager.listar_entes()
    vehiculos_prestables = db_manager.listar_vehiculos_prestables(usuario_id)
    movimientos = db_manager.listar_movimientos(usuario_id=usuario_id)
    alertas = db_manager.movimientos_con_alerta(
        movimientos,
        app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
    )
    return {
        "movimientos": alertas,
        "vehiculos": vehiculos,
        "usuarios": usuarios,
        "entes": entes,
        "vehiculos_prestables": vehiculos_prestables,
        "items": items,
        "today": date.today().isoformat(),
    }


def _build_admin_context(app: Flask, db_manager: DatabaseManager) -> dict:
    items = _filtrar_vehiculos(db_manager.listar_items(activos=False))
    movimientos = db_manager.listar_movimientos()
    alertas = db_manager.movimientos_con_alerta(
        movimientos,
        app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
    )
    vehiculos = db_manager.listar_vehiculos()
    responsables = db_manager.listar_responsables()
    total_stock = sum(int(item.get("stock_total") or 0) for item in items)
    total_disponible = sum(int(item.get("stock_disponible") or 0) for item in items)
    en_uso = sum(
        1
        for mov in alertas
        if mov.data.get("fecha_entrega") and not mov.data.get("devuelto")
    )
    pendientes = sum(1 for mov in alertas if not mov.data.get("fecha_entrega"))
    total_alertas = sum(1 for mov in alertas if mov.alerta)
    return {
        "items": items,
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

        context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
        return render_template("dashboard.html", usuario=session.get("nombre"), **context)

    @app.route("/admin")
    def admin():
        if session.get("rol") not in {"admin", "monitor"}:
            return redirect(url_for("dashboard"))
        context = _build_admin_context(app, db_manager)
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

        item_id = request.form.get("item_id")
        cantidad = request.form.get("cantidad") or "1"
        vehiculo_id = request.form.get("vehiculo_id")
        responsable_usuario_id = request.form.get("responsable_usuario_id")
        no_pasajeros_txt = request.form.get("no_pasajeros", "").strip()
        pasajeros_ids = [pid for pid in request.form.getlist("pasajeros_ids") if pid]
        ruta_destinos = [clave for clave in request.form.getlist("ruta_destinos") if clave]
        motivo = request.form.get("motivo_salida", "").strip()
        tipo_notificacion_id = request.form.get("tipo_notificacion_id")
        observaciones = request.form.get("observaciones")
        fecha = request.form.get("fecha_solicitud")

        if not vehiculo_id or not vehiculo_id.isdigit():
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="Falta seleccionar la unidad.",
                **context,
            )

        if not responsable_usuario_id or not responsable_usuario_id.isdigit():
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="Falta seleccionar al responsable del vehiculo.",
                **context,
            )

        if not no_pasajeros_txt.isdigit():
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El numero de pasajeros debe ser numerico.",
                **context,
            )
        no_pasajeros = int(no_pasajeros_txt)
        if no_pasajeros < 0:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El numero de pasajeros no puede ser menor a cero.",
                **context,
            )
        if no_pasajeros > 4:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El numero de pasajeros no puede exceder 4 (maximo 5 ocupantes incluyendo al conductor).",
                **context,
            )

        if len(pasajeros_ids) != no_pasajeros:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El numero de pasajeros debe coincidir con los nombres seleccionados.",
                **context,
            )
        if any(not pid.isdigit() for pid in pasajeros_ids):
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="La lista de pasajeros no es valida.",
                **context,
            )

        if not ruta_destinos:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="Falta seleccionar la ruta destino.",
                **context,
            )

        motivos_validos = {
            "Notificación de Oficio",
            "Revisión de Auditoría",
            "Entrega de Recepción",
            "Compulsas",
            "Inspección Física",
        }
        if motivo not in motivos_validos:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="El motivo de salida no es valido.",
                **context,
            )

        if not tipo_notificacion_id:
            notificaciones = db_manager.listar_notificaciones()
            if notificaciones:
                tipo_notificacion_id = notificaciones[0].get("id")

        vehiculos = db_manager.listar_vehiculos(usuario_id=session.get("usuario_id"))
        vehiculo = next(
            (v for v in vehiculos if str(v.get("id")) == str(vehiculo_id)),
            None,
        )
        if not vehiculo:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="La unidad seleccionada no es valida.",
                **context,
            )

        placa_upper = (vehiculo.get("placa") or "").upper()

        if not item_id and placa_upper:
            items = _filtrar_vehiculos(db_manager.listar_items())
            item = next(
                (i for i in items if (i.get("sigla") or "").upper() == placa_upper),
                None,
            )
            if item:
                item_id = item.get("id")

        if not item_id or not tipo_notificacion_id:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error="Faltan datos requeridos para completar la solicitud.",
                **context,
            )

        ok, data = db_manager.crear_movimiento(
            int(item_id),
            session["usuario_id"],
            ruta_destinos[0],
            int(cantidad),
            "",
            None,
            observaciones,
            None,
            int(vehiculo_id),
            int(responsable_usuario_id),
            int(no_pasajeros),
            [int(pid) for pid in pasajeros_ids],
            ruta_destinos,
            motivo,
            int(tipo_notificacion_id) if tipo_notificacion_id else None,
            fecha_solicitud=fecha,
        )

        if not ok:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                error=data.get("mensaje") if isinstance(data, dict) else data,
                **context,
            )

        return redirect(url_for("reporte_movimiento", mov_id=data["movimiento_id"]))

    @app.route("/prestamos/solicitar", methods=["POST"])
    def solicitar_prestamo():
        if not session.get("autenticado"):
            return redirect(url_for("login"))
        if session.get("rol") != "user":
            return redirect(url_for("dashboard"))

        raw_val = request.form.get("prestamo_vehiculo", "")
        responsable_usuario_id = request.form.get("prestamo_responsable_usuario_id")
        no_pasajeros_txt = request.form.get("prestamo_no_pasajeros", "").strip()
        pasajeros_ids = [pid for pid in request.form.getlist("prestamo_pasajeros_ids") if pid]
        fechas_prestamo = [fecha for fecha in request.form.getlist("prestamo_fechas") if fecha]
        ruta_destinos = [clave for clave in request.form.getlist("prestamo_ruta_destinos") if clave]
        motivo = request.form.get("prestamo_motivo_salida", "").strip()
        notas = request.form.get("prestamo_notas")
        if ":" not in raw_val:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="Seleccione el vehiculo y propietario para el prestamo.",
                **context,
            )

        vehiculo_txt, propietario_txt = raw_val.split(":", 1)
        if not vehiculo_txt.isdigit() or not propietario_txt.isdigit():
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="Seleccion no valida para el prestamo.",
                **context,
            )

        if not responsable_usuario_id or not responsable_usuario_id.isdigit():
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="Falta seleccionar al responsable del vehiculo.",
                **context,
            )

        if not no_pasajeros_txt.isdigit():
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="El numero de pasajeros debe ser numerico.",
                **context,
            )
        no_pasajeros = int(no_pasajeros_txt)
        if no_pasajeros < 0:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="El numero de pasajeros no puede ser menor a cero.",
                **context,
            )
        if no_pasajeros > 4:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="El numero de pasajeros no puede exceder 4 (maximo 5 ocupantes incluyendo al conductor).",
                **context,
            )

        if len(pasajeros_ids) != no_pasajeros:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="El numero de pasajeros debe coincidir con los nombres seleccionados.",
                **context,
            )
        if any(not pid.isdigit() for pid in pasajeros_ids):
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="La lista de pasajeros no es valida.",
                **context,
            )

        if not fechas_prestamo:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="Falta seleccionar al menos un dia para el prestamo.",
                **context,
            )
        fechas_parsed = []
        for fecha in fechas_prestamo:
            try:
                fechas_parsed.append(datetime.strptime(fecha, "%Y-%m-%d").date())
            except ValueError:
                context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
                return render_template(
                    "dashboard.html",
                    usuario=session.get("nombre"),
                    prestamo_error="Formato de fecha no valido para el prestamo.",
                    **context,
                )
        hoy = date.today()
        lunes = hoy - timedelta(days=hoy.weekday())
        viernes = lunes + timedelta(days=4)
        for fecha in fechas_parsed:
            if fecha < hoy or fecha < lunes or fecha > viernes or fecha.weekday() > 4:
                context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
                return render_template(
                    "dashboard.html",
                    usuario=session.get("nombre"),
                    prestamo_error="Las fechas deben estar dentro de la semana actual (lunes a viernes).",
                    **context,
                )

        if not ruta_destinos:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="Falta seleccionar la ruta destino.",
                **context,
            )

        motivos_validos = {
            "Notificación de Oficio",
            "Revisión de Auditoría",
            "Entrega de Recepción",
            "Compulsas",
            "Inspección Física",
        }
        if motivo not in motivos_validos:
            context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
            return render_template(
                "dashboard.html",
                usuario=session.get("nombre"),
                prestamo_error="El motivo de salida no es valido.",
                **context,
            )

        ok, mensaje = db_manager.solicitar_prestamo(
            session.get("usuario_id"),
            int(propietario_txt),
            int(vehiculo_txt),
            int(responsable_usuario_id),
            int(no_pasajeros),
            [int(pid) for pid in pasajeros_ids],
            [fecha.isoformat() for fecha in sorted(set(fechas_parsed))],
            ruta_destinos,
            motivo,
            notas,
        )
        context = _build_dashboard_context(app, db_manager, session.get("usuario_id"))
        return render_template(
            "dashboard.html",
            usuario=session.get("nombre"),
            prestamo_ok=mensaje if ok else None,
            prestamo_error=None if ok else mensaje,
            **context,
        )

    @app.route("/gestor")
    def gestor():
        return redirect(url_for("admin"))

    @app.route("/inventario/nuevo", methods=["POST"])
    def inventario_nuevo():
        if session.get("rol") != "admin":
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
                "admin.html",
                **_build_admin_context(app, db_manager),
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
            )

        return redirect(url_for("admin"))

    @app.route("/movimientos/<int:mov_id>/entregar", methods=["POST"])
    def movimientos_entregar(mov_id: int):
        if session.get("rol") not in {"admin", "monitor"}:
            return redirect(url_for("dashboard"))
        db_manager.marcar_entregado(mov_id, session.get("usuario_id"))
        return redirect(url_for("admin"))

    @app.route("/movimientos/<int:mov_id>/rechazar", methods=["POST"])
    def movimientos_rechazar(mov_id: int):
        if session.get("rol") not in {"admin", "monitor"}:
            return redirect(url_for("dashboard"))
        db_manager.marcar_rechazado(mov_id, session.get("usuario_id"))
        return redirect(url_for("admin"))

    @app.route("/movimientos/<int:mov_id>/devolver", methods=["POST"])
    def movimientos_devolver(mov_id: int):
        if session.get("rol") != "admin":
            return redirect(url_for("dashboard"))
        db_manager.marcar_devuelto(mov_id, session.get("usuario_id"))
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
        fecha = request.args.get("fecha") or date.today().isoformat()
        movimientos = db_manager.listar_movimientos_entregados(
            session.get("usuario_id"),
            fecha,
        )
        fecha_larga = _fecha_larga_es(fecha)
        return render_template(
            "reporte_diario.html",
            movimientos=movimientos,
            fecha_larga=fecha_larga,
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
