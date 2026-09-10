"""
Vehiculos - Aplicacion Flask
============================
Control de vehiculos para auditoria institucional.
"""

import csv
import logging
import secrets
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from flask import (
    Flask,
    abort,
    flash,
    get_flashed_messages,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.exceptions import HTTPException

from config import get_config
from scripts.utils import (
    CATEGORIA_VEHICULO_OBRA,
    CATEGORIAS_VEHICULO,
    DatabaseManager,
    MOTIVOS_SALIDA_VALIDOS,
    normalizar_rol as _normalizar_rol,
)
from sso import clear_sso_cookie, read_sso_username, set_sso_cookie


USUARIOS_SOLICITUD_OBRA = {"mike", "ramos"}
USUARIOS_SOLICITUD_COMPLETA = {"luis"}
MOVIMIENTOS_RAPIDOS = {"mike": "Mike", "luis": "Luis Felipe"}
USUARIOS_CONTRASENAS = Path(__file__).with_name("usuarios_contrasenas.csv")


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

    log_file = app.config.get("LOG_FILE", "logs/app.log")
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


def _filtrar_movimientos_hoy(
    movimientos: List[dict],
    hoy: Optional[date] = None,
    incluir_prestamos_pendientes: bool = False,
) -> List[dict]:
    hoy_iso = (hoy or date.today()).isoformat()
    filtrados = []
    for mov in movimientos:
        if _fecha_iso_desde_texto(mov.get("fecha_solicitud")) == hoy_iso:
            filtrados.append(mov)
            continue
        if (
            incluir_prestamos_pendientes
            and mov.get("tipo") == "prestamo"
            and (mov.get("estado") or "").strip().upper() == "PENDIENTE"
        ):
            filtrados.append(mov)
    return filtrados


def _fecha_referencia_registros(fecha_txt: Optional[str] = None) -> date:
    if fecha_txt:
        try:
            return datetime.strptime(fecha_txt, "%Y-%m-%d").date()
        except ValueError:
            pass
    base = date.today()
    if base.weekday() == 5:  # Sabado
        return base + timedelta(days=2)
    if base.weekday() == 6:  # Domingo
        return base + timedelta(days=1)
    return base


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


def _agrupar_vehiculos_por_resguardante(vehiculos: List[dict]) -> List[dict]:
    def _resguardante(vehiculo: dict) -> str:
        return (vehiculo.get("propietarios_nombres") or "Sin asignacion").strip() or "Sin asignacion"

    ordenados = sorted(
        vehiculos,
        key=lambda item: (
            _resguardante(item).casefold(),
            (item.get("categoria") or "").casefold(),
            (item.get("placa") or "").casefold(),
        ),
    )
    grupos = []
    for vehiculo in ordenados:
        resguardante = _resguardante(vehiculo)
        if not grupos or grupos[-1]["resguardante"] != resguardante:
            grupos.append({"resguardante": resguardante, "vehiculos": []})
        grupos[-1]["vehiculos"].append(vehiculo)
    return grupos


def _agrupar_vehiculos_por_categoria(vehiculos: List[dict]) -> List[dict]:
    grupos_por_categoria = {categoria: [] for categoria in CATEGORIAS_VEHICULO}
    ordenados = sorted(
        vehiculos,
        key=lambda item: (
            CATEGORIAS_VEHICULO.index(item.get("categoria"))
            if item.get("categoria") in CATEGORIAS_VEHICULO else len(CATEGORIAS_VEHICULO),
            (item.get("placa") or "").casefold(),
        ),
    )
    for vehiculo in ordenados:
        categoria = vehiculo.get("categoria") if vehiculo.get("categoria") in CATEGORIAS_VEHICULO else "Sin categoria"
        grupos_por_categoria.setdefault(categoria, []).append(vehiculo)
    return [
        {"categoria": categoria, "vehiculos": items}
        for categoria, items in grupos_por_categoria.items()
        if items
    ]


def _empty_emergencia_form() -> dict:
    return {
        "resguardante_nombre": "",
        "vehiculo_id": "",
        "responsable_auditor_id": "",
        "no_pasajeros": "0",
        "pasajeros_ids": [],
        "ruta_destinos": [],
        "motivo_salida": "",
    }


def _emergencia_form_from_request() -> dict:
    return {
        "resguardante_nombre": request.form.get("resguardante_nombre", "").strip(),
        "vehiculo_id": request.form.get("vehiculo_id", "").strip(),
        "responsable_auditor_id": request.form.get("responsable_auditor_id", "").strip(),
        "no_pasajeros": request.form.get("no_pasajeros", "").strip() or "0",
        "pasajeros_ids": [pid for pid in request.form.getlist("pasajeros_ids") if pid],
        "ruta_destinos": [clave for clave in request.form.getlist("ruta_destinos") if clave],
        "motivo_salida": request.form.get("motivo_salida", "").strip(),
    }


def _solicitudes_bloqueadas(usuario: Optional[str]) -> bool:
    return False


def _es_visor_omar(usuario: Optional[str]) -> bool:
    return (usuario or "").strip().lower() == "omar"


def _puede_solicitar_unidades_obra(usuario: Optional[str]) -> bool:
    return (usuario or "").strip().lower() in USUARIOS_SOLICITUD_OBRA


def _puede_solicitar_todo(usuario: Optional[str]) -> bool:
    return (usuario or "").strip().lower() in USUARIOS_SOLICITUD_COMPLETA


def _listar_vehiculos_solicitables(
    db_manager: DatabaseManager,
    usuario_id: int,
    usuario: Optional[str],
) -> List[dict]:
    if _puede_solicitar_todo(usuario):
        return db_manager.listar_vehiculos()
    if _puede_solicitar_unidades_obra(usuario):
        return db_manager.listar_vehiculos_por_categoria(CATEGORIA_VEHICULO_OBRA)
    return db_manager.listar_vehiculos(usuario_id=usuario_id)


def _vehiculo_frecuente(movimientos: List[dict], vehiculos: List[dict]) -> Optional[dict]:
    conteos = Counter(
        mov.get("vehiculo_id")
        for mov in movimientos
        if mov.get("vehiculo_id") and not mov.get("rechazado")
    )
    if not conteos:
        return None
    vehiculo_id, pedidos = conteos.most_common(1)[0]
    vehiculo = next((item for item in vehiculos if item.get("id") == vehiculo_id), None)
    return {**vehiculo, "pedidos": pedidos} if vehiculo else None


def _plantilla_movimiento_rapido(
    db_manager: DatabaseManager,
    usuario: str,
    vehiculos: List[dict],
) -> Optional[dict]:
    usuario_id = db_manager.obtener_usuario_id(usuario)
    if not usuario_id:
        return None
    historial = [
        mov for mov in db_manager.listar_movimientos(usuario_id=usuario_id)
        if mov.get("tipo") == "movimiento"
    ]
    vehiculo = _vehiculo_frecuente(historial, vehiculos)
    if not vehiculo:
        return None
    referencia = next(
        (
            mov for mov in historial
            if mov.get("vehiculo_id") == vehiculo["id"] and not mov.get("rechazado")
        ),
        None,
    )
    solicitud = (
        db_manager.obtener_solicitud_edicion("movimiento", referencia["id"])
        if referencia else None
    )
    if not solicitud or not solicitud.get("responsable_auditor_id"):
        return None
    return {
        **solicitud,
        **vehiculo,
        "usuario": usuario,
        "usuario_id": usuario_id,
        "usuario_nombre_corto": MOVIMIENTOS_RAPIDOS[usuario],
    }


def _build_dashboard_context(
    app: Flask,
    db_manager: DatabaseManager,
    usuario_id: int,
    fecha_solicitud: Optional[str] = None,
) -> dict:
    fecha_txt = _normalizar_fecha_solicitud(fecha_solicitud)
    fecha_referencia = _fecha_referencia_registros(fecha_solicitud)
    usuario_actual = session.get("usuario")
    permite_solicitud_completa = _puede_solicitar_todo(usuario_actual)
    # Garantiza que el usuario actual exista en el catalogo de auditores
    # para poder seleccionarlo como responsable o pasajero.
    db_manager.asegurar_auditor_usuario(usuario_id)
    vehiculos = _listar_vehiculos_solicitables(
        db_manager,
        usuario_id,
        usuario_actual,
    )
    permite_unidades_obra = _puede_solicitar_unidades_obra(usuario_actual)
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

    if permite_solicitud_completa:
        responsables = auditores_ofs
        auditores = auditores_ofs
    else:
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

    entes = db_manager.listar_entes()
    vehiculos_prestables = db_manager.listar_vehiculos_prestables(usuario_id, fecha_txt)
    movimientos = _filtrar_movimientos_hoy(
        db_manager.listar_movimientos(usuario_id=usuario_id),
        fecha_referencia,
        incluir_prestamos_pendientes=not bool(fecha_solicitud),
    )
    alertas = db_manager.movimientos_con_alerta(
        movimientos,
        app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
    )
    return {
        "movimientos": alertas,
        "mis_movimientos": movimientos,
        "vehiculos": vehiculos,
        "vehiculos_grupo_label": (
            "Todas las unidades"
            if permite_solicitud_completa
            else "Unidades de obra" if permite_unidades_obra else "Mis unidades"
        ),
        "responsables": responsables,
        "auditores": auditores,
        "auditores_ofs": auditores_ofs,
        "personal_global": permite_solicitud_completa,
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
        "fecha_referencia_registros": fecha_referencia.isoformat(),
    }


def _build_admin_context(
    app: Flask,
    db_manager: DatabaseManager,
    rol: Optional[str] = None,
    fecha_consulta: Optional[str] = None,
    emergencia_form: Optional[dict] = None,
) -> dict:
    movimientos, fecha_filtro = _listar_movimientos_admin_filtrados(
        db_manager,
        rol,
        fecha_consulta,
    )
    alertas = db_manager.movimientos_con_alerta(
        movimientos,
        app.config.get("ALERTA_DIAS_NO_DEVUELTO", 7),
    )
    movimientos_validables = sum(1 for mov in movimientos if _es_movimiento_validable(mov))
    fecha_hoy = date.today().isoformat()
    if rol == "monitor":
        vehiculos = db_manager.listar_vehiculos_con_propietarios()
        movimientos_rapidos = [
            plantilla
            for usuario_rapido in MOVIMIENTOS_RAPIDOS
            if (plantilla := _plantilla_movimiento_rapido(
                db_manager,
                usuario_rapido,
                vehiculos,
            ))
        ]
        vehiculos_emergencia = db_manager.listar_vehiculos_disponibles_con_propietarios(fecha_hoy)
        ocupados_auditores = db_manager.obtener_auditores_ocupados(fecha_hoy)
        auditores_emergencia = [
            item for item in db_manager.listar_auditores()
            if item.get("id") not in ocupados_auditores
        ]
        entes = db_manager.listar_entes()
    else:
        vehiculos = db_manager.listar_vehiculos()
        movimientos_rapidos = []
        vehiculos_emergencia = []
        auditores_emergencia = []
        entes = []
    usuarios_resguardo = (
        db_manager.listar_usuarios_resguardo()
        if rol == "monitor" else []
    )
    usuarios_resguardo_por_categoria = {categoria: [] for categoria in CATEGORIAS_VEHICULO}
    for usuario_item in usuarios_resguardo:
        categoria_resguardo = usuario_item.get("categoria_resguardo")
        usuarios_resguardo_por_categoria.setdefault(categoria_resguardo, []).append(usuario_item)
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
        "vehiculos_reasignacion_grupos": _agrupar_vehiculos_por_resguardante(vehiculos),
        "vehiculos_unidades_grupos": _agrupar_vehiculos_por_categoria(vehiculos),
        "responsables": responsables,
        "total_stock": total_stock,
        "total_disponible": total_disponible,
        "total_vehiculos": len(vehiculos),
        "movimientos_en_uso": en_uso,
        "movimientos_pendientes": pendientes,
        "movimientos_validables": movimientos_validables,
        "movimientos_rapidos": movimientos_rapidos,
        "movimientos_alerta": total_alertas,
        "today": date.today().isoformat(),
        "fecha_consulta": fecha_filtro,
        "vehiculos_emergencia": vehiculos_emergencia,
        "usuarios_resguardo": usuarios_resguardo,
        "usuarios_resguardo_por_categoria": usuarios_resguardo_por_categoria,
        "categorias_vehiculo": CATEGORIAS_VEHICULO,
        "auditores_emergencia": auditores_emergencia,
        "entes": entes,
        "motivos_salida": MOTIVOS_SALIDA_VALIDOS,
        "emergencia_habilitada": rol == "monitor" and fecha_filtro == fecha_hoy,
        "emergencia_form": emergencia_form or _empty_emergencia_form(),
        "recordatorios": _build_recordatorios_context(db_manager) if rol == "monitor" else {},
    }


def _build_recordatorios_context(
    db_manager: DatabaseManager,
    error=None,
    usuario_editado=None,
    telefono_ingresado="",
    credenciales=None,
) -> dict:
    manana = datetime.now(ZoneInfo("America/Mexico_City")).date() + timedelta(days=1)
    fecha, _ = _limites_semana_laboral(manana, total=1)
    fecha_recordatorio = _fecha_larga_es(fecha.isoformat()).lower()
    mensaje_recordatorio = (
        "Buenas tardes, {nombre}.\n\n"
        f"Se le recuerda solicitar un vehículo para el día {fecha_recordatorio} en la plataforma:\n"
        "https://vehiculos.omar-xyz.shop\n\n"
        "Gracias por su atención."
    )
    mensaje_editable = (
        "Buenas tardes, {nombre completo}.\n\n"
        "Se le recuerda solicitar un vehículo para el día {fecha} en la plataforma:\n"
        "https://vehiculos.omar-xyz.shop\n\n"
        "Gracias por su atención."
    )
    usuarios = db_manager.listar_usuarios_recordatorios()
    for usuario in usuarios:
        telefono = usuario["telefono_whatsapp"]
        if telefono:
            usuario["chat_url"] = "https://wa.me/521" + telefono[3:]
            mensaje = mensaje_recordatorio.format(nombre=usuario["nombre"])
            usuario["recordatorio_url"] = usuario["chat_url"] + "?" + urlencode({"text": mensaje})
            mensaje_credenciales = (
                f"Usuario: {usuario['usuario']}\n"
                f"Contraseña: {_clave_control_interno(usuario['usuario'])}"
            )
            usuario["credenciales_url"] = usuario["chat_url"] + "?" + urlencode({"text": mensaje_credenciales})
    if "csrf_recordatorios" not in session:
        session["csrf_recordatorios"] = secrets.token_urlsafe(32)
    return {
        "usuarios": usuarios,
        "error": error,
        "usuario_editado": usuario_editado,
        "telefono_ingresado": telefono_ingresado,
        "mensaje_recordatorio": mensaje_editable,
        "fecha_recordatorio": fecha_recordatorio,
        "credenciales": credenciales,
    }


def _clave_control_interno(usuario: str) -> str:
    if not USUARIOS_CONTRASENAS.exists():
        return ""
    with USUARIOS_CONTRASENAS.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if (row.get("usuario") or "").strip().casefold() == (usuario or "").strip().casefold():
                return (row.get("contrasena") or "").strip()
    return ""


def _listar_movimientos_admin_filtrados(
    db_manager: DatabaseManager,
    rol: Optional[str] = None,
    fecha_consulta: Optional[str] = None,
) -> Tuple[List[dict], str]:
    movimientos = db_manager.listar_movimientos()
    fecha_filtro = ""
    if fecha_consulta:
        fecha_referencia = _fecha_referencia_registros(fecha_consulta)
        fecha_filtro = fecha_referencia.isoformat()
        movimientos = _filtrar_movimientos_hoy(
            movimientos,
            fecha_referencia,
        )
    elif rol == "monitor":
        fecha_referencia = _fecha_referencia_registros()
        fecha_filtro = fecha_referencia.isoformat()
        movimientos = _filtrar_movimientos_hoy(
            movimientos,
            fecha_referencia,
        )
    return movimientos, fecha_filtro


def _es_movimiento_validable(movimiento: dict) -> bool:
    return not movimiento.get("fecha_entrega") and not movimiento.get("rechazado")


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
    def _destino_por_rol(rol: str) -> str:
        if rol == "monitor":
            return "admin"
        return "dashboard"

    def _aplicar_sesion_usuario(user: dict) -> str:
        rol = _normalizar_rol(user["rol"])
        session.permanent = True
        session.update({
            "usuario_id": user["id"],
            "usuario": user["usuario"],
            "nombre": user["nombre"],
            "rol": rol,
            "entes": user["entes"],
            "autenticado": True,
        })
        return rol

    def _hidratar_sesion_desde_sso() -> bool:
        user = db_manager.get_usuario_por_username(read_sso_username())
        if not user:
            return False
        session.clear()
        _aplicar_sesion_usuario(user)
        return True

    @app.before_request
    def verificar_autenticacion():
        if session.get("autenticado"):
            session["rol"] = _normalizar_rol(session.get("rol"))
        libres = {"login", "login_alias", "static", "health_check"}
        if request.endpoint not in libres and not session.get("autenticado"):
            _hidratar_sesion_desde_sso()
        if request.endpoint not in libres and not session.get("autenticado"):
            return redirect(url_for("login"))

    @app.route("/", methods=["GET", "POST"])
    def login():
        if session.get("autenticado") or _hidratar_sesion_desde_sso():
            return set_sso_cookie(
                redirect(url_for(_destino_por_rol(session.get("rol", "")))),
                session.get("usuario"),
            )
        if request.method == "POST":
            usuario = request.form.get("usuario", "").strip()
            clave = request.form.get("clave", "").strip()
            user = db_manager.get_usuario(usuario, clave)
            if not user:
                return render_template("login.html", error="Credenciales inválidas")

            session.clear()
            rol = _aplicar_sesion_usuario(user)
            return set_sso_cookie(redirect(url_for(_destino_por_rol(rol))), user["usuario"])
        return render_template("login.html")

    @app.route("/login")
    def login_alias():
        return redirect(url_for("login"))

    @app.route("/logout")
    def logout():
        session.clear()
        return clear_sso_cookie(redirect(url_for("login")))

    @app.route("/dashboard")
    def dashboard():
        if session.get("rol") == "monitor":
            return redirect(url_for("admin"))

        fecha = request.args.get("fecha")
        context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
        context["solicitudes_bloqueadas"] = _solicitudes_bloqueadas(session.get("usuario"))
        if _es_visor_omar(session.get("usuario")):
            context["movimientos_usuarios_observados"] = _filtrar_movimientos_hoy(
                db_manager.listar_movimientos_por_usuarios(["ramos", "mike"]),
                _fecha_referencia_registros(fecha),
                incluir_prestamos_pendientes=not bool(fecha),
            )
        return render_template(
            "dashboard.html",
            usuario=session.get("nombre"),
            tipo_solicitud="normal",
            **context,
        )

    @app.route("/admin")
    def admin():
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))
        context = _build_admin_context(
            app,
            db_manager,
            session.get("rol"),
            request.args.get("fecha"),
        )
        context["monitor_active_panel"] = (
            request.args.get("tab")
            if request.args.get("tab") in {"movimientos", "emergencia", "reasignacion", "unidades", "recordatorios"}
            else "movimientos"
        )
        for categoria, texto in get_flashed_messages(with_categories=True):
            if categoria == "error":
                context["error"] = texto
            elif categoria == "mensaje":
                context["mensaje"] = texto
        return render_template(
            "admin.html",
            usuario=session.get("nombre"),
            rol=session.get("rol"),
            **context,
        )

    @app.route("/monitoreo")
    def monitoreo():
        return redirect(url_for("admin"))

    def _render_recordatorios(error=None, usuario_editado=None, telefono_ingresado="", credenciales=None):
        response = app.make_response(render_template(
            "recordatorios.html",
            **_build_recordatorios_context(
                db_manager,
                error,
                usuario_editado,
                telefono_ingresado,
                credenciales,
            ),
        ))
        response.headers["Cache-Control"] = "no-store"
        return response

    def _render_recordatorios_admin(error=None, usuario_editado=None, telefono_ingresado="", credenciales=None, status=200):
        context = _build_admin_context(app, db_manager, session.get("rol"), request.form.get("fecha"))
        context["recordatorios"] = _build_recordatorios_context(
            db_manager,
            error,
            usuario_editado,
            telefono_ingresado,
            credenciales,
        )
        response = app.make_response(render_template(
            "admin.html",
            usuario=session.get("nombre"),
            rol=session.get("rol"),
            monitor_active_panel="recordatorios",
            **context,
        ))
        response.headers["Cache-Control"] = "no-store"
        return response, status

    def _render_recordatorios_or_admin(*args, status=200, **kwargs):
        if request.form.get("origen") == "admin":
            return _render_recordatorios_admin(*args, status=status, **kwargs)
        response = _render_recordatorios(*args, **kwargs)
        return (response, status) if status != 200 else response

    @app.get("/configuracion/recordatorios")
    def recordatorios():
        if session.get("rol") != "monitor":
            abort(403)
        return _render_recordatorios()

    def _verificar_csrf_recordatorios():
        if session.get("rol") != "monitor":
            abort(403)
        token = session.get("csrf_recordatorios", "")
        recibido = request.form.get("csrf_token", "")
        if not token or not secrets.compare_digest(token.encode(), recibido.encode()):
            abort(400, description="La sesión del formulario venció. Recargue Recordatorios e intente de nuevo.")

    @app.post("/configuracion/recordatorios/<int:usuario_id>")
    def recordatorios_guardar(usuario_id: int):
        _verificar_csrf_recordatorios()
        telefono = request.form.get("telefono_whatsapp", "")
        try:
            actualizado = db_manager.guardar_telefono_whatsapp(usuario_id, telefono)
        except ValueError as error:
            return _render_recordatorios_or_admin(str(error), usuario_id, telefono, status=400)
        if not actualizado:
            abort(404)
        flash("Teléfono guardado." if telefono.strip() else "Teléfono eliminado.", "mensaje")
        if request.form.get("origen") == "admin":
            return redirect(url_for("admin", tab="recordatorios"))
        return redirect(url_for("recordatorios"))

    @app.post("/configuracion/credenciales")
    def credenciales_emergencia():
        _verificar_csrf_recordatorios()
        if request.form.get("confirmar_restablecimiento") != "1":
            return _render_recordatorios_or_admin(
                credenciales={"error": "Confirme el cambio de contraseña antes de continuar."},
                status=400,
            )
        usuario_id = request.form.get("usuario_id", "")
        destinatario = next((
            usuario for usuario in db_manager.listar_usuarios_recordatorios()
            if str(usuario["id"]) == usuario_id
        ), None)
        if not destinatario:
            abort(404)
        telefono = destinatario["telefono_whatsapp"]
        if not telefono:
            return _render_recordatorios_or_admin(
                credenciales={"error": "Guarde el teléfono del destinatario antes de generar credenciales."},
                status=400,
            )
        clave = _clave_control_interno(destinatario["usuario"])
        mensaje = (
            f"Usuario: {destinatario['usuario']}\n"
            f"Contraseña: {clave}"
        )
        app.logger.info(
            "Credenciales preparadas: administrador=%s destinatario=%s",
            session.get("usuario_id"), destinatario["id"],
        )
        return _render_recordatorios_or_admin(credenciales={
            "nombre": destinatario["nombre"],
            "telefono": telefono,
            "mensaje": mensaje,
            "url": "https://wa.me/521" + telefono[3:] + "?" + urlencode({"text": mensaje}),
        })

    def _redirect_admin_con_fecha(fecha_txt: Optional[str] = None):
        fecha_raw = (fecha_txt or "").strip()
        if not fecha_raw:
            return redirect(url_for("admin"))
        return redirect(url_for("admin", fecha=_normalizar_fecha_solicitud(fecha_raw)))

    @app.route(
        "/solicitudes/<tipo>/<int:solicitud_id>/editar",
        methods=["GET", "POST"],
    )
    def solicitudes_editar(tipo: str, solicitud_id: int):
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))

        fecha_consulta = request.values.get("fecha_consulta", "").strip()
        solicitud = db_manager.obtener_solicitud_edicion(tipo, solicitud_id)
        if not solicitud:
            flash("La solicitud no existe.", "error")
            return _redirect_admin_con_fecha(fecha_consulta)

        error = None
        if request.method == "POST":
            vehiculo_txt = request.form.get("vehiculo_id", "").strip()
            responsable_txt = request.form.get("responsable_auditor_id", "").strip()
            pasajeros_txt = [pid.strip() for pid in request.form.getlist("pasajeros_ids") if pid.strip()]
            ruta_destinos = [clave.strip() for clave in request.form.getlist("ruta_destinos") if clave.strip()]
            fecha_solicitud = request.form.get("fecha_solicitud", "").strip()
            motivo_salida = request.form.get("motivo_salida", "").strip()

            if not vehiculo_txt.isdigit():
                error = "Falta seleccionar la unidad."
            elif not responsable_txt.isdigit():
                error = "Falta seleccionar al responsable del vehiculo."
            elif any(not pid.isdigit() for pid in pasajeros_txt):
                error = "La lista de acompanantes no es valida."
            else:
                ok, mensaje = db_manager.actualizar_solicitud_reporte(
                    tipo,
                    solicitud_id,
                    int(vehiculo_txt),
                    fecha_solicitud,
                    int(responsable_txt),
                    [int(pid) for pid in pasajeros_txt],
                    ruta_destinos,
                    motivo_salida,
                    session.get("usuario_id"),
                )
                if ok:
                    flash(mensaje, "mensaje")
                    return _redirect_admin_con_fecha(fecha_solicitud)
                error = mensaje

            solicitud.update({
                "vehiculo_id": int(vehiculo_txt) if vehiculo_txt.isdigit() else None,
                "fecha_solicitud": fecha_solicitud,
                "responsable_auditor_id": (
                    int(responsable_txt) if responsable_txt.isdigit() else None
                ),
                "pasajeros_ids": [int(pid) for pid in pasajeros_txt if pid.isdigit()],
                "ruta_destinos": ruta_destinos,
                "motivo_salida": motivo_salida,
            })

        return render_template(
            "editar_solicitud.html",
            solicitud=solicitud,
            vehiculos=db_manager.listar_vehiculos_con_propietarios(),
            auditores=db_manager.listar_auditores(),
            entes=db_manager.listar_entes(),
            motivos_salida=MOTIVOS_SALIDA_VALIDOS,
            fecha_consulta=fecha_consulta or solicitud.get("fecha_solicitud"),
            error=error,
        )

    @app.route("/movimientos/validar-todos", methods=["POST"])
    def movimientos_validar_todos():
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))

        fecha = request.form.get("fecha", "").strip()
        movimientos, _ = _listar_movimientos_admin_filtrados(
            db_manager,
            session.get("rol"),
            fecha,
        )
        pendientes = [mov for mov in movimientos if _es_movimiento_validable(mov)]

        if not pendientes:
            flash("No hay movimientos pendientes por validar.", "error")
            return _redirect_admin_con_fecha(fecha)

        validados = 0
        errores = []
        for mov in pendientes:
            mov_id = int(mov["id"])
            etiqueta = mov.get("folio") or f"Movimiento {mov_id}"
            if mov.get("tipo") == "prestamo":
                ok, mensaje = db_manager.marcar_prestamo_validado(
                    mov_id,
                    session.get("usuario_id"),
                )
            else:
                ok, mensaje = db_manager.marcar_entregado(
                    mov_id,
                    session.get("usuario_id"),
                )
            if ok:
                validados += 1
            else:
                errores.append(f"{etiqueta}: {mensaje}")

        if errores:
            detalle = "; ".join(errores[:5])
            if len(errores) > 5:
                detalle = f"{detalle}; y {len(errores) - 5} mas."
            flash(
                f"Se validaron {validados} de {len(pendientes)} movimientos. No se validaron: {detalle}",
                "error",
            )
            return _redirect_admin_con_fecha(fecha)

        flash(f"Se validaron {validados} movimientos.", "mensaje")
        return _redirect_admin_con_fecha(fecha)

    @app.route("/movimientos/rapido/<usuario>", methods=["POST"])
    def movimientos_rapido(usuario: str):
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))

        usuario = usuario.strip().lower()
        fecha = _fecha_iso_desde_texto(request.form.get("fecha"))
        if usuario not in MOVIMIENTOS_RAPIDOS or not fecha:
            flash("El movimiento rapido solicitado no es valido.", "error")
            return _redirect_admin_con_fecha(fecha)

        plantilla = _plantilla_movimiento_rapido(
            db_manager,
            usuario,
            db_manager.listar_vehiculos(),
        )
        if not plantilla:
            flash("No hay un movimiento anterior valido para repetir.", "error")
            return _redirect_admin_con_fecha(fecha)

        pasajeros_ids = plantilla.get("pasajeros_ids") or []
        ruta_destinos = plantilla.get("ruta_destinos") or []
        ok, data = db_manager.crear_movimiento(
            plantilla["usuario_id"],
            ruta_destinos[0] if ruta_destinos else "",
            1,
            "",
            None,
            f"Alta rapida validada por monitor; referencia {plantilla['folio']}.",
            None,
            plantilla["id"],
            plantilla["responsable_auditor_id"],
            "auditor",
            len(pasajeros_ids),
            pasajeros_ids,
            ruta_destinos,
            plantilla["motivo_salida"],
            fecha_solicitud=fecha,
        )
        if not ok:
            flash(data.get("mensaje") if isinstance(data, dict) else str(data), "error")
            return _redirect_admin_con_fecha(fecha)

        movimiento_id = data["movimiento_id"]
        ok, mensaje = db_manager.marcar_entregado(movimiento_id, session.get("usuario_id"))
        if not ok:
            flash(f"El movimiento se creo pendiente: {mensaje}", "error")
            return _redirect_admin_con_fecha(fecha)

        flash(
            f"Movimiento de {plantilla['usuario_nombre_corto']} con {plantilla['placa']} agregado y validado.",
            "mensaje",
        )
        return _redirect_admin_con_fecha(fecha)

    @app.route("/movimientos/emergencia", methods=["POST"])
    def movimientos_emergencia():
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))

        form_data = _emergencia_form_from_request()
        fecha_emergencia = date.today().isoformat()

        def _render_error(mensaje: str):
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(
                    app,
                    db_manager,
                    session.get("rol"),
                    fecha_emergencia,
                    emergencia_form=form_data,
                ),
            )

        if not form_data["vehiculo_id"].isdigit():
            return _render_error("Falta seleccionar una unidad disponible.")
        if not form_data["responsable_auditor_id"].isdigit():
            return _render_error("Falta seleccionar al responsable del vehiculo.")
        if not form_data["no_pasajeros"].isdigit():
            return _render_error("El numero de pasajeros debe ser numerico.")

        no_pasajeros = int(form_data["no_pasajeros"])
        if no_pasajeros < 0:
            return _render_error("El numero de pasajeros no puede ser menor a cero.")
        if no_pasajeros > 4:
            return _render_error("El numero de pasajeros no puede exceder 4.")
        if any(not pid.isdigit() for pid in form_data["pasajeros_ids"]):
            return _render_error("La lista de auditores no es valida.")
        if len(form_data["pasajeros_ids"]) != no_pasajeros:
            return _render_error("El numero de pasajeros debe coincidir con los auditores seleccionados.")
        if not form_data["ruta_destinos"]:
            return _render_error("Falta seleccionar la ruta destino.")
        if form_data["motivo_salida"] not in MOTIVOS_SALIDA_VALIDOS:
            return _render_error("El motivo de salida no es valido.")

        ok, data = db_manager.crear_movimiento_emergencia(
            session.get("usuario_id"),
            int(form_data["vehiculo_id"]),
            int(form_data["responsable_auditor_id"]),
            no_pasajeros,
            [int(pid) for pid in form_data["pasajeros_ids"]],
            form_data["ruta_destinos"],
            form_data["motivo_salida"],
            fecha_solicitud=fecha_emergencia,
        )
        if not ok:
            return _render_error(data.get("mensaje") if isinstance(data, dict) else str(data))

        return redirect(url_for("reporte_movimiento", mov_id=data["movimiento_id"]))

    @app.route("/vehiculos/reasignar", methods=["POST"])
    def vehiculos_reasignar():
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))

        fecha = request.form.get("fecha", "").strip()
        vehiculo_id_txt = request.form.get("vehiculo_id", "").strip()
        usuario_destino_txt = request.form.get("usuario_destino_id", "").strip()
        categoria = request.form.get("categoria", "").strip()
        modo = request.form.get("modo", "mover").strip()

        def _render_error(mensaje: str):
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol"), fecha),
            )

        if not vehiculo_id_txt.isdigit():
            return _render_error("Falta seleccionar el vehiculo a reasignar.")
        if not usuario_destino_txt.isdigit():
            return _render_error("Falta seleccionar el nuevo resguardante.")
        if categoria and categoria not in CATEGORIAS_VEHICULO:
            return _render_error("La categoria del vehiculo no es valida.")

        ok, mensaje = db_manager.reasignar_vehiculo(
            int(vehiculo_id_txt),
            int(usuario_destino_txt),
            categoria,
            modo,
        )
        if not ok:
            return _render_error(mensaje)
        return _redirect_admin_con_fecha(fecha)

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

        if motivo not in MOTIVOS_SALIDA_VALIDOS:
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
            ok, resultado = db_manager.solicitar_prestamo(
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
            if not ok:
                context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
                return render_template(
                    "dashboard.html",
                    usuario=session.get("nombre"),
                    error=resultado.get("mensaje") if isinstance(resultado, dict) else resultado,
                    tipo_solicitud=tipo_solicitud,
                    **context,
                )
            prestamo_id = resultado.get("prestamo_id") if isinstance(resultado, dict) else None
            if not prestamo_id:
                context = _build_dashboard_context(app, db_manager, session.get("usuario_id"), fecha)
                return render_template(
                    "dashboard.html",
                    usuario=session.get("nombre"),
                    error="No se pudo preparar el comprobante del prestamo.",
                    tipo_solicitud=tipo_solicitud,
                    **context,
                )
            return redirect(url_for("reporte_prestamo", prestamo_id=prestamo_id))

        vehiculos = _listar_vehiculos_solicitables(
            db_manager,
            session.get("usuario_id"),
            session.get("usuario"),
        )
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
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))

        placa = request.form.get("placa")
        marca = request.form.get("marca")
        modelo = request.form.get("modelo")
        categoria = request.form.get("categoria")

        ok, mensaje = db_manager.crear_vehiculo(
            placa,
            marca,
            modelo,
            categoria,
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
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))
        fecha = request.form.get("fecha", "").strip()
        ok, mensaje = db_manager.marcar_entregado(mov_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol"), fecha),
            )
        return _redirect_admin_con_fecha(fecha)

    @app.route("/movimientos/<int:mov_id>/rechazar", methods=["POST"])
    def movimientos_rechazar(mov_id: int):
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))
        fecha = request.form.get("fecha", "").strip()
        ok, mensaje = db_manager.marcar_rechazado(mov_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol"), fecha),
            )
        return _redirect_admin_con_fecha(fecha)

    @app.route("/prestamos/<int:prestamo_id>/validar", methods=["POST"])
    def prestamos_validar(prestamo_id: int):
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))
        fecha = request.form.get("fecha", "").strip()
        ok, mensaje = db_manager.marcar_prestamo_validado(prestamo_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol"), fecha),
            )
        return _redirect_admin_con_fecha(fecha)

    @app.route("/prestamos/<int:prestamo_id>/rechazar", methods=["POST"])
    def prestamos_rechazar(prestamo_id: int):
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))
        fecha = request.form.get("fecha", "").strip()
        ok, mensaje = db_manager.marcar_prestamo_rechazado(prestamo_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol"), fecha),
            )
        return _redirect_admin_con_fecha(fecha)

    @app.route("/movimientos/<int:mov_id>/devolver", methods=["POST"])
    def movimientos_devolver(mov_id: int):
        if session.get("rol") != "monitor":
            return redirect(url_for("dashboard"))
        fecha = request.form.get("fecha", "").strip()
        ok, mensaje = db_manager.marcar_devuelto(mov_id, session.get("usuario_id"))
        if not ok:
            return render_template(
                "admin.html",
                usuario=session.get("nombre"),
                rol=session.get("rol"),
                error=mensaje,
                **_build_admin_context(app, db_manager, session.get("rol"), fecha),
            )
        return _redirect_admin_con_fecha(fecha)

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
        can_print = session.get("rol") == "monitor"
        fecha_larga = _fecha_larga_es(movimiento.get("fecha_solicitud"))
        return render_template(
            "reporte.html",
            movimiento=movimiento,
            fecha_larga=fecha_larga,
            can_print=can_print,
        )

    @app.route("/reporte/prestamo/<int:prestamo_id>")
    def reporte_prestamo(prestamo_id: int):
        if not session.get("autenticado"):
            return redirect(url_for("login"))
        movimiento = db_manager.obtener_prestamo(prestamo_id)
        if not movimiento:
            return redirect(url_for("dashboard"))
        can_print = session.get("rol") == "monitor"
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
            return redirect(url_for("dashboard"))
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
        "Miércoles",
        "Jueves",
        "Viernes",
        "Sábado",
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
