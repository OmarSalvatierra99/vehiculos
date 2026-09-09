"""
Utilidades y acceso a datos para Inventarios OFS.
"""

import hashlib
import logging
import random
import re
import sqlite3
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

logger = logging.getLogger("INVENTARIOS")

MOTIVOS_SALIDA_VALIDOS = (
    "Notificación de Oficio",
    "Revisión de Auditoría",
    "Entrega de Recepción",
    "Acta de Cierre",
    "Compulsas",
    "Inspección Física",
)

PLACAS_SOLO_PROPIETARIO = {"XVZ-353-C"}

CATEGORIA_VEHICULO_OBRA = "Obra"
CATEGORIA_VEHICULO_FINANCIERO = "Financiero"
CATEGORIAS_VEHICULO = (
    CATEGORIA_VEHICULO_OBRA,
    CATEGORIA_VEHICULO_FINANCIERO,
)
PLACAS_OBRA = {
    "XB-3501-D",
    "XB-3502-D",
    "XB-3503-D",
    "XC-8407-C",
    "XVZ-335-C",
    "XXK-741-D",
}
USUARIOS_OBRA = {"ramos", "mike", "omar"}

AUDITOR_RUBEN_MENDEZ_CANONICO = "C.P. Rubén Jesús Méndez Arámbula"
AUDITOR_RUBEN_MENDEZ_CLAVE = "RUBEN_JESUS_MENDEZ_ARAMBULA"


def _normalizar_header(valor: str) -> str:
    if not valor:
        return ""
    limpio = re.sub(r"[^A-Z0-9]", "", str(valor).upper())
    return limpio


def _normalizar_clave(valor: str) -> str:
    if not valor:
        return ""
    normalizado = unicodedata.normalize("NFKD", str(valor))
    ascii_txt = "".join(ch for ch in normalizado if not unicodedata.combining(ch))
    ascii_txt = ascii_txt.upper()
    ascii_txt = re.sub(r"[^A-Z0-9]+", "_", ascii_txt).strip("_")
    return ascii_txt


def _clave_ente_canonica(valor: Optional[str]) -> str:
    clave = _normalizar_clave(valor)
    if not clave:
        return ""
    if clave in {"SM", "SMYT"} or "MOVILIDAD_Y_TRANSPORTE" in clave:
        return ENTE_CLAVE_SM
    return clave


def _normalizar_categoria_vehiculo(valor: Optional[str]) -> str:
    clave = _normalizar_clave(valor)
    if clave in {"OBRA", "OBRAS", "AREA_DE_OBRA"}:
        return CATEGORIA_VEHICULO_OBRA
    if clave in {"FINANCIERO", "FINANCIERA", "FINANZAS", "AREA_FINANCIERA"}:
        return CATEGORIA_VEHICULO_FINANCIERO
    return CATEGORIA_VEHICULO_FINANCIERO


def _categoria_por_placa(placa: str) -> str:
    if (placa or "").strip().upper() in PLACAS_OBRA:
        return CATEGORIA_VEHICULO_OBRA
    return CATEGORIA_VEHICULO_FINANCIERO


def _categoria_resguardo_usuario(usuario: Optional[str]) -> str:
    usuario_txt = (usuario or "").strip().lower()
    if usuario_txt in USUARIOS_OBRA:
        return CATEGORIA_VEHICULO_OBRA
    return CATEGORIA_VEHICULO_FINANCIERO


def _variantes_ente(valor: Optional[str]) -> List[str]:
    if valor is None:
        return []
    texto = str(valor).strip()
    if not texto:
        return []
    candidatos = [
        texto,
        texto.upper(),
        _clave_ente_canonica(texto),
        _normalizar_clave(texto),
    ]
    return list(dict.fromkeys(candidato for candidato in candidatos if candidato))


def _parse_date(valor: Optional[str]) -> Optional[str]:
    if not valor:
        return None
    if isinstance(valor, (datetime, date)):
        return valor.strftime("%Y-%m-%d")
    valor = str(valor).strip()
    if not valor:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(valor, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _hoy_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _split_ruta_destino(ruta_destino: Optional[str]) -> List[str]:
    if not ruta_destino:
        return []
    return [token.strip() for token in re.split(r"\s*(?:->|,)\s*", str(ruta_destino)) if token.strip()]


def _hora_mexico_desde_created_at(created_at: Optional[str]) -> str:
    if not created_at:
        return "-"

    texto = str(created_at).strip()
    if not texto:
        return "-"

    formatos = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    )
    dt = None
    for fmt in formatos:
        try:
            dt = datetime.strptime(texto, fmt)
            break
        except ValueError:
            continue
    if dt is None:
        return "-"

    # CURRENT_TIMESTAMP en SQLite se guarda en UTC.
    dt_utc = dt.replace(tzinfo=timezone.utc)
    if ZoneInfo is None:
        return dt_utc.strftime("%H:%M")
    return dt_utc.astimezone(ZoneInfo("America/Mexico_City")).strftime("%H:%M")


def _limites_dias_habiles(base: Optional[date] = None) -> Tuple[date, date]:
    inicio = base or date.today()
    if inicio.weekday() >= 5:
        dias_hasta_lunes = (7 - inicio.weekday()) % 7
        inicio = inicio + timedelta(days=dias_hasta_lunes)

    fin = inicio
    dias_contados = 1
    while dias_contados < 5:
        fin += timedelta(days=1)
        if fin.weekday() < 5:
            dias_contados += 1
    return inicio, fin


def _hash_password(clave: str) -> str:
    return hashlib.sha256(clave.encode()).hexdigest()


ENTES_MANUALES = [
    "PODER EJECUTIVO DEL ESTADO DE TLAXCALA",
    "DESPACHO DE LA GOBERNADORA",
    "SECRETARÍA DE LA FUNCIÓN PÚBLICA",
    "SECRETARÍA DE IMPULSO AGROPECUARIO",
    "COORDINACIÓN DE COMUNICACIÓN",
    "SECRETARÍA DE MEDIO AMBIENTE",
    "SECRETARÍA DE CULTURA",
    "SECRETARÍA DE LAS MUJERES",
    "SECRETARÍA DE ORDENAMIENTO TERRITORIAL Y VIVIENDA",
    "SECRETARÍA DE SEGURIDAD CIUDADANA",
    "COORDINACIÓN GENERAL DE PLANEACIÓN E INVERSIÓN",
    "SECRETARÍA DE BIENESTAR",
    "SECRETARÍA DE GOBIERNO",
    "SECRETARÍA DE TRABAJO Y COMPETITIVIDAD",
    "CONSEJERÍA JURÍDICA DEL EJECUTIVO",
    "COORDINACIÓN ESTATAL DE PROTECCIÓN CIVIL",
    "SECRETARIADO EJECUTIVO DEL SISTEMA ESTATAL DE SEGURIDAD PÚBLICA",
    "INSTITUTO TLAXCALTECA DE DESARROLLO TAURINO",
    "INSTITUTO TLAXCALTECA DE ASISTENCIA ESPECIALIZADA A LA SALUD",
    "COMISIÓN ESTATAL DE ARBITRAJE MÉDICO",
    "CASA DE LAS ARTESANÍAS DE TLAXCALA",
    "PROCURADURÍA DE PROTECCIÓN AL AMBIENTE DEL ESTADO DE TLAXCALA",
    "INSTITUTO DE FAUNA SILVESTRE PARA EL ESTADO DE TLAXCALA",
    "OFICIALÍA MAYOR DE GOBIERNO",
    "SECRETARÍA DE FINANZAS",
    "SECRETARÍA DE DESARROLLO ECONÓMICO",
    "SECRETARÍA DE TURISMO",
    "SECRETARÍA DE INFRAESTRUCTURA",
    "SECRETARÍA DE EDUCACIÓN PÚBLICA",
    "SECRETARÍA DE MOVILIDAD Y TRANSPORTE",
    "COORDINACIÓN DE RADIO, CINE Y TELEVISIÓN",
    "EL COLEGIO DE TLAXCALA, A.C.",
    "FIDEICOMISO DE LA CIUDAD INDUSTRIAL DE XICOTÉNCATL",
    "COMISIÓN EJECUTIVA DE ATENCIÓN A VÍCTIMAS DEL ESTADO DE TLAXCALA",
    "FONDO MACRO PARA EL DESARROLLO INTEGRAL DE TLAXCALA",
    "INSTITUTO DE CAPACITACIÓN PARA EL TRABAJO DEL ESTADO DE TLAXCALA",
    "INSTITUTO DE CATASTRO DEL ESTADO DE TLAXCALA",
    "INSTITUTO DEL DEPORTE DE TLAXCALA",
    "INSTITUTO TECNOLÓGICO SUPERIOR DE TLAXCO",
    "INSTITUTO TLAXCALTECA DE LA INFRAESTRUCTURA FÍSICA EDUCATIVA",
    "PODER LEGISLATIVO DEL ESTADO DE TLAXCALA",
    "INSTITUTO TLAXCALTECA DE LA JUVENTUD",
    "INSTITUTO TLAXCALTECA PARA LA EDUCACIÓN DE LOS ADULTOS",
    "ORGANISMO PÚBLICO DESCENTRALIZADO SALUD DE TLAXCALA",
    "PATRONATO CENTRO DE REHABILITACIÓN INTEGRAL Y ESCUELA EN TERAPIA FÍSICA Y REHABILITACIÓN",
    "PATRONATO “LA LIBERTAD CENTRO CULTURAL DE APIZACO”",
    "PENSIONES CIVILES DEL ESTADO DE TLAXCALA",
    "SISTEMA ESTATAL PARA EL DESARROLLO INTEGRAL DE LA FAMILIA",
    "UNIDAD DE SERVICIOS EDUCATIVOS DEL ESTADO DE TLAXCALA",
    "UNIVERSIDAD POLITÉCNICA DE TLAXCALA",
    "UNIVERSIDAD POLITÉCNICA DE TLAXCALA REGIÓN PONIENTE",
    "PODER JUDICIAL DEL ESTADO DE TLAXCALA",
    "UNIVERSIDAD TECNOLÓGICA DE TLAXCALA",
    "UNIVERSIDAD INTERCULTURAL DE TLAXCALA",
    "ARCHIVO GENERAL E HISTÓRICO DEL ESTADO DE TLAXCALA",
    "TRIBUNAL DE JUSTICIA ADMINISTRATIVA DEL ESTADO DE TLAXCALA",
    "UNIVERSIDAD AUTÓNOMA DE TLAXCALA",
    "COMISIÓN ESTATAL DE DERECHOS HUMANOS",
    "INSTITUTO TLAXCALTECA DE ELECCIONES",
    "INSTITUTO DE ACCESO A LA INFORMACIÓN PÚBLICA Y PROTECCIÓN DE DATOS PERSONALES DEL ESTADO DE TLAXCALA",
    "TRIBUNAL DE CONCILIACIÓN Y ARBITRAJE DEL ESTADO DE TLAXCALA",
    "TRIBUNAL ELECTORAL DE TLAXCALA",
    "CENTRO DE CONCILIACIÓN LABORAL DEL ESTADO DE TLAXCALA",
    "FISCALÍA GENERAL DE JUSTICIA DEL ESTADO DE TLAXCALA",
    "SECRETARÍA EJECUTIVA DEL SISTEMA ANTICORRUPCIÓN DEL ESTADO DE TLAXCALA",
    "PATRONATO PARA LAS EXPOSICIONES Y FERIAS EN LA CIUDAD DE TLAXCALA",
    "COMISIÓN ESTATAL DEL AGUA Y SANEAMIENTO DEL ESTADO DE TLAXCALA",
    "COLEGIO DE BACHILLERES DEL ESTADO DE TLAXCALA",
    "COLEGIO DE EDUCACIÓN PROFESIONAL TÉCNICA DEL ESTADO DE TLAXCALA",
    "COLEGIO DE ESTUDIOS CIENTÍFICOS Y TECNOLÓGICOS DEL ESTADO DE TLAXCALA",
    "CONSEJO ESTATAL DE POBLACIÓN",
    "COMISIÓN DE AGUA POTABLE Y ALCANTARILLADO DEL MUNICIPIO DE HUAMANTLA",
    "COMISIÓN DE AGUA POTABLE Y ALCANTARILLADO DEL MUNICIPIO DE APIZACO",
    "COMISIÓN DE AGUA POTABLE Y ALCANTARILLADO DEL MUNICIPIO DE CHIAUTEMPAN",
    "COMISIÓN DE AGUA POTABLE Y ALCANTARILLADO DEL MUNICIPIO DE ZACATELCO",
    "COMISIÓN DE POTABLE Y Y ALCANTARILLADO DEL MUNICIPIO TLAXCALA",
]

MUNICIPIOS_MANUALES = [
    "ACUAMANALA DE MIGUEL HIDALGO",
    "CONTLA DE JUAN CUAMATZI",
    "CUAPIAXTLA",
    "CUAXOMULCO",
    "EL CARMEN TEQUEXQUITLA",
    "EMILIANO ZAPATA",
    "ESPAÑITA",
    "HUAMANTLA",
    "HUEYOTLIPAN",
    "IXTACUIXTLA DE MARIANO MATAMOROS",
    "IXTENCO",
    "ATLTZAYANCA",
    "LA MAGDALENA TLALTELULCO",
    "LÁZARO CÁRDENAS",
    "MAZATECOCHCO DE JOSÉ MARÍA MORELOS",
    "MUÑOZ DE DOMINGO ARENAS",
    "NANACAMILPA DE MARIANO ARISTA",
    "NATIVITAS",
    "PANOTLA",
    "PAPALOTLA DE XICOHTÉNCATL",
    "SAN DAMIÁN TEXOLOC",
    "SAN FRANCISCO TETLANOHCAN",
    "AMAXAC DE GUERRERO",
    "SAN JERÓNIMO ZACUALPAN",
    "SAN JOSÉ TEACALCO",
    "SAN JUAN HUACTZINCO",
    "SAN LORENZO AXOCOMANITLA",
    "SAN LUCAS TECOPILCO",
    "SAN PABLO DEL MONTE",
    "SANCTÓRUM DE LÁZARO CÁRDENAS",
    "SANTA ANA NOPALUCAN",
    "SANTA APOLONIA TEACALCO",
    "SANTA CATARINA AYOMETLA",
    "APETATITLÁN DE ANTONIO CARVAJAL",
    "SANTA CRUZ QUILEHTLA",
    "SANTA CRUZ TLAXCALA",
    "SANTA ISABEL XILOXOXTLA",
    "TENANCINGO",
    "TEOLOCHOLCO",
    "TEPETITLA DE LARDIZÁBAL",
    "TEPEYANCO",
    "TERRENATE",
    "TETLA DE LA SOLIDARIDAD",
    "TETLATLAHUCA",
    "APIZACO",
    "TLAXCALA",
    "TLAXCO",
    "TOCATLÁN",
    "TOTOLAC",
    "TZOMPANTEPEC",
    "XALOZTOC",
    "XALTOCAN",
    "XICOHTZINCO",
    "YAUHQUEMEHCAN",
    "ZACATELCO",
    "ATLANGATEPEC",
    "ZITLALTÉPEC DE TRINIDAD SÁNCHEZ SANTOS",
    "BENITO JUÁREZ",
    "CALPULALPAN",
    "CHIAUTEMPAN",
]

ENTE_CLAVE_SM = "SMYT"
ENTE_NOMBRE_SM = "Secretaría de Movilidad y Transporte (SMyT)"


@dataclass
class MovimientoRow:
    data: Dict
    alerta: bool = False


class DatabaseManager:
    def __init__(self, db_path: str, catalogos_dir: str):
        self.db_path = db_path
        self.catalogos_dir = Path(catalogos_dir)
        logger.info("Base de datos en uso: %s", Path(self.db_path).resolve())
        self._init_db()
        self._migrate_schema()
        self._ensure_vehiculos_columns()
        self._ensure_usuarios_columns()
        self._ensure_movimientos_columns()
        self._ensure_prestamos_columns()
        self._normalizar_catalogo_entes()
        self._seed_usuarios()
        self._seed_vehiculos()
        self._seed_usuarios_vehiculos()
        self._ensure_usuarios_vehiculos_unicos()
        self._seed_responsables()
        self._seed_auditores()
        self._seed_responsables_auditores()
        self._ensure_auditor_ruben_mendez()
        self._seed_resguardantes()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                usuario TEXT UNIQUE NOT NULL,
                clave TEXT NOT NULL,
                rol TEXT NOT NULL DEFAULT 'usuario',
                puesto TEXT DEFAULT '',
                entes TEXT NOT NULL DEFAULT 'TODOS',
                activo INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS usuarios_personal (
                resguardante_id INTEGER NOT NULL,
                personal_id INTEGER NOT NULL,
                PRIMARY KEY (resguardante_id, personal_id),
                FOREIGN KEY(resguardante_id) REFERENCES usuarios(id),
                FOREIGN KEY(personal_id) REFERENCES usuarios(id)
            );

            CREATE TABLE IF NOT EXISTS entes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                clave TEXT UNIQUE NOT NULL,
                nombre TEXT NOT NULL,
                tipo TEXT NOT NULL,
                activo INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS movimientos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folio TEXT UNIQUE NOT NULL,
                usuario_id INTEGER NOT NULL,
                ente_clave TEXT NOT NULL,
                fecha_solicitud TEXT NOT NULL,
                fecha_entrega TEXT,
                fecha_devolucion TEXT,
                cantidad INTEGER NOT NULL,
                receptor_nombre TEXT NOT NULL,
                firma_recepcion TEXT,
                devuelto INTEGER DEFAULT 0,
                observaciones TEXT,
                resguardante_nombre TEXT,
                resguardante_id INTEGER,
                placa_unidad TEXT,
                marca TEXT,
                modelo TEXT,
                responsable_vehiculo TEXT,
                vehiculo_id INTEGER,
                responsable_id INTEGER,
                no_pasajeros INTEGER,
                ruta_destino TEXT,
                motivo_salida TEXT,
                es_emergencia INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
            );

            CREATE TABLE IF NOT EXISTS vehiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                placa TEXT UNIQUE NOT NULL,
                modelo TEXT NOT NULL,
                marca TEXT NOT NULL,
                categoria TEXT NOT NULL DEFAULT 'Financiero',
                activo INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS responsables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE NOT NULL,
                activo INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS resguardantes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE NOT NULL,
                activo INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS auditores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE NOT NULL,
                activo INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS responsables_auditores (
                responsable_id INTEGER NOT NULL,
                auditor_id INTEGER NOT NULL,
                orden INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (responsable_id, auditor_id),
                FOREIGN KEY(responsable_id) REFERENCES responsables(id),
                FOREIGN KEY(auditor_id) REFERENCES auditores(id)
            );

            CREATE TABLE IF NOT EXISTS movimientos_auditores (
                movimiento_id INTEGER NOT NULL,
                auditor_id INTEGER NOT NULL,
                PRIMARY KEY (movimiento_id, auditor_id),
                FOREIGN KEY(movimiento_id) REFERENCES movimientos(id),
                FOREIGN KEY(auditor_id) REFERENCES auditores(id)
            );

            CREATE TABLE IF NOT EXISTS movimientos_destinos (
                movimiento_id INTEGER NOT NULL,
                ente_clave TEXT NOT NULL,
                orden INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (movimiento_id, ente_clave, orden),
                FOREIGN KEY(movimiento_id) REFERENCES movimientos(id),
                FOREIGN KEY(ente_clave) REFERENCES entes(clave)
            );


            CREATE TABLE IF NOT EXISTS movimientos_eventos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movimiento_id INTEGER NOT NULL,
                usuario_id INTEGER,
                evento TEXT NOT NULL,
                fecha TEXT NOT NULL,
                notas TEXT,
                FOREIGN KEY(movimiento_id) REFERENCES movimientos(id)
            );

            CREATE TABLE IF NOT EXISTS usuarios_vehiculos (
                usuario_id INTEGER NOT NULL,
                vehiculo_id INTEGER NOT NULL,
                PRIMARY KEY (usuario_id, vehiculo_id),
                UNIQUE (vehiculo_id),
                FOREIGN KEY(usuario_id) REFERENCES usuarios(id),
                FOREIGN KEY(vehiculo_id) REFERENCES vehiculos(id)
            );

            CREATE TABLE IF NOT EXISTS prestamos_vehiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                solicitante_id INTEGER NOT NULL,
                propietario_id INTEGER NOT NULL,
                vehiculo_id INTEGER NOT NULL,
                fecha_solicitud TEXT NOT NULL,
                estado TEXT NOT NULL DEFAULT 'PENDIENTE',
                notas TEXT,
                fechas_solicitadas TEXT,
                FOREIGN KEY(solicitante_id) REFERENCES usuarios(id),
                FOREIGN KEY(propietario_id) REFERENCES usuarios(id),
                FOREIGN KEY(vehiculo_id) REFERENCES vehiculos(id)
            );
        """)
        conn.commit()
        conn.close()

    def _migrate_schema(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(movimientos)")
        existentes = {row["name"] for row in cur.fetchall()}
        columnas_obsoletas = {"item_id", "no_inventario", "tipo_notificacion_id", "auditores_nombres"}
        if existentes & columnas_obsoletas:
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS movimientos_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    folio TEXT UNIQUE NOT NULL,
                    usuario_id INTEGER NOT NULL,
                    ente_clave TEXT NOT NULL,
                    fecha_solicitud TEXT NOT NULL,
                    fecha_entrega TEXT,
                    fecha_devolucion TEXT,
                    cantidad INTEGER NOT NULL,
                    receptor_nombre TEXT NOT NULL,
                    firma_recepcion TEXT,
                    devuelto INTEGER DEFAULT 0,
                    observaciones TEXT,
                    resguardante_nombre TEXT,
                    resguardante_id INTEGER,
                    placa_unidad TEXT,
                    marca TEXT,
                    modelo TEXT,
                    responsable_vehiculo TEXT,
                    vehiculo_id INTEGER,
                    responsable_id INTEGER,
                    no_pasajeros INTEGER,
                    ruta_destino TEXT,
                    motivo_salida TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(usuario_id) REFERENCES usuarios(id),
                    FOREIGN KEY(vehiculo_id) REFERENCES vehiculos(id)
                );
            """)
            columnas_objetivo = [
                "id",
                "folio",
                "usuario_id",
                "ente_clave",
                "fecha_solicitud",
                "fecha_entrega",
                "fecha_devolucion",
                "cantidad",
                "receptor_nombre",
                "firma_recepcion",
                "devuelto",
                "observaciones",
                "resguardante_nombre",
                "resguardante_id",
                "placa_unidad",
                "marca",
                "modelo",
                "responsable_vehiculo",
                "vehiculo_id",
                "responsable_id",
                "no_pasajeros",
                "ruta_destino",
                "motivo_salida",
                "created_at",
            ]
            columnas_migradas = [col for col in columnas_objetivo if col in existentes]
            columnas_txt = ", ".join(columnas_migradas)
            cur.execute(f"""
                INSERT INTO movimientos_new ({columnas_txt})
                SELECT {columnas_txt}
                FROM movimientos
            """)
            cur.execute("DROP TABLE movimientos")
            cur.execute("ALTER TABLE movimientos_new RENAME TO movimientos")

        cur.execute("DROP TABLE IF EXISTS inventario_items")
        cur.execute("DROP TABLE IF EXISTS notificaciones")
        cur.execute("DROP TABLE IF EXISTS movimientos_pasajeros")
        conn.commit()
        conn.close()

    def _ensure_vehiculos_columns(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(vehiculos)")
        existentes = {row["name"] for row in cur.fetchall()}
        categoria_agregada = False
        if "categoria" not in existentes:
            cur.execute("ALTER TABLE vehiculos ADD COLUMN categoria TEXT NOT NULL DEFAULT 'Financiero'")
            categoria_agregada = True
        cur.execute("""
            UPDATE vehiculos
            SET categoria = ?
            WHERE TRIM(COALESCE(categoria, '')) = ''
        """, (CATEGORIA_VEHICULO_FINANCIERO,))
        if categoria_agregada:
            placeholders = ",".join(["?"] * len(PLACAS_OBRA))
            cur.execute(f"""
                UPDATE vehiculos
                SET categoria = ?
                WHERE UPPER(placa) IN ({placeholders})
            """, (CATEGORIA_VEHICULO_OBRA, *sorted(PLACAS_OBRA)))
        conn.commit()
        conn.close()

    def _ensure_usuarios_vehiculos_unicos(self) -> None:
        conn = self._connect()
        cur = conn.cursor()

        cur.execute("""
            SELECT vehiculo_id
            FROM usuarios_vehiculos
            GROUP BY vehiculo_id
            HAVING COUNT(*) > 1
        """)
        duplicados = [row["vehiculo_id"] for row in cur.fetchall()]
        for vehiculo_id in duplicados:
            cur.execute("""
                SELECT uv.rowid AS rel_id, uv.usuario_id
                FROM usuarios_vehiculos uv
                JOIN usuarios u ON u.id = uv.usuario_id
                WHERE uv.vehiculo_id=?
                ORDER BY
                    CASE WHEN LOWER(u.usuario) = 'omar' THEN 0 ELSE 1 END,
                    uv.rowid
            """, (vehiculo_id,))
            relaciones = cur.fetchall()
            if not relaciones:
                continue
            conservar_id = relaciones[0]["rel_id"]
            cur.execute("""
                DELETE FROM usuarios_vehiculos
                WHERE vehiculo_id=? AND rowid<>?
            """, (vehiculo_id, conservar_id))

        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_usuarios_vehiculos_vehiculo_unico
            ON usuarios_vehiculos (vehiculo_id)
        """)
        conn.commit()
        conn.close()

    def _ensure_usuarios_columns(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(usuarios)")
        existentes = {row["name"] for row in cur.fetchall()}
        if "puesto" not in existentes:
            cur.execute("ALTER TABLE usuarios ADD COLUMN puesto TEXT DEFAULT ''")
        conn.commit()
        conn.close()

    def _ensure_movimientos_columns(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(movimientos)")
        existentes = {row["name"] for row in cur.fetchall()}
        columnas = [
            ("resguardante_nombre", "TEXT"),
            ("resguardante_id", "INTEGER"),
            ("placa_unidad", "TEXT"),
            ("marca", "TEXT"),
            ("modelo", "TEXT"),
            ("responsable_vehiculo", "TEXT"),
            ("vehiculo_id", "INTEGER"),
            ("responsable_id", "INTEGER"),
            ("no_pasajeros", "INTEGER"),
            ("ruta_destino", "TEXT"),
            ("motivo_salida", "TEXT"),
            ("es_emergencia", "INTEGER DEFAULT 0"),
        ]
        for nombre, tipo in columnas:
            if nombre not in existentes:
                cur.execute(f"ALTER TABLE movimientos ADD COLUMN {nombre} {tipo}")
        conn.commit()
        conn.close()

    def _ensure_prestamos_columns(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(prestamos_vehiculos)")
        existentes = {row["name"] for row in cur.fetchall()}
        columnas = [
            ("responsable_id", "INTEGER"),
            ("responsable_nombre", "TEXT"),
            ("no_pasajeros", "INTEGER"),
            ("pasajeros_ids", "TEXT"),
            ("ruta_destino", "TEXT"),
            ("motivo_salida", "TEXT"),
            ("fechas_solicitadas", "TEXT"),
        ]
        for nombre, tipo in columnas:
            if nombre not in existentes:
                cur.execute(f"ALTER TABLE prestamos_vehiculos ADD COLUMN {nombre} {tipo}")
        conn.commit()
        conn.close()

    def _normalizar_catalogo_entes(self) -> None:
        conn = self._connect()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, clave
            FROM entes
            WHERE UPPER(clave) IN ('SM', 'SMYT')
        """)
        rows = cur.fetchall()
        if rows:
            sm_id = next((row["id"] for row in rows if (row["clave"] or "").upper() == "SM"), None)
            smyt_id = next((row["id"] for row in rows if (row["clave"] or "").upper() == ENTE_CLAVE_SM), None)

            cur.execute("""
                UPDATE movimientos
                SET ente_clave = ?
                WHERE UPPER(COALESCE(ente_clave, '')) = 'SM'
            """, (ENTE_CLAVE_SM,))
            cur.execute("""
                UPDATE movimientos_destinos
                SET ente_clave = ?
                WHERE UPPER(COALESCE(ente_clave, '')) = 'SM'
            """, (ENTE_CLAVE_SM,))

            cur.execute("""
                UPDATE movimientos
                SET ruta_destino = REPLACE(ruta_destino, 'Secretaría de Movilidad y Transporte (SM)', ?)
                WHERE ruta_destino LIKE '%Secretaría de Movilidad y Transporte (SM)%'
            """, (ENTE_NOMBRE_SM,))
            cur.execute("""
                UPDATE prestamos_vehiculos
                SET ruta_destino = REPLACE(ruta_destino, 'Secretaría de Movilidad y Transporte (SM)', ?)
                WHERE ruta_destino LIKE '%Secretaría de Movilidad y Transporte (SM)%'
            """, (ENTE_NOMBRE_SM,))

            cur.execute("""
                SELECT id, entes
                FROM usuarios
                WHERE UPPER(COALESCE(entes, '')) LIKE '%SM%'
            """)
            for row in cur.fetchall():
                tokens = [token.strip() for token in (row["entes"] or "").split(",") if token.strip()]
                normalizados = []
                for token in tokens:
                    canonico = _clave_ente_canonica(token)
                    if canonico not in normalizados:
                        normalizados.append(canonico)
                nuevo_valor = ",".join(normalizados) or "TODOS"
                if nuevo_valor != (row["entes"] or ""):
                    cur.execute("""
                        UPDATE usuarios
                        SET entes = ?
                        WHERE id = ?
                    """, (nuevo_valor, row["id"]))

            if smyt_id:
                cur.execute("""
                    UPDATE entes
                    SET nombre = ?, activo = 1
                    WHERE id = ?
                """, (ENTE_NOMBRE_SM, smyt_id))
                if sm_id and sm_id != smyt_id:
                    cur.execute("DELETE FROM entes WHERE id = ?", (sm_id,))
            elif sm_id:
                cur.execute("""
                    UPDATE entes
                    SET clave = ?, nombre = ?, activo = 1
                    WHERE id = ?
                """, (ENTE_CLAVE_SM, ENTE_NOMBRE_SM, sm_id))

            conn.commit()

        conn.close()

    def _seed_usuarios(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM usuarios")
        total = cur.fetchone()[0]

        usuarios_path = self.catalogos_dir / "Usuarios_SASP_2025.xlsx"
        usuarios = []

        try:
            from openpyxl import load_workbook
        except ImportError:
            load_workbook = None

        if load_workbook and usuarios_path.exists():
            wb = load_workbook(usuarios_path, read_only=True, data_only=True)
            ws = wb[wb.sheetnames[0]]
            rows = ws.iter_rows(min_row=1, values_only=True)
            headers = next(rows, [])
            header_map = {
                _normalizar_header(h): idx
                for idx, h in enumerate(headers or [])
                if h
            }

            def get_val(row, keys):
                for key in keys:
                    for h, idx in header_map.items():
                        if key in h:
                            return row[idx]
                return None

            for row in rows:
                nombre = get_val(row, ["NOMBRE"])
                usuario = get_val(row, ["USUARIO"])
                clave = get_val(row, ["CLAVE", "PASSWORD"])
                entes = get_val(row, ["ENTES", "ENTE"])
                rol = get_val(row, ["ROL", "PERFIL"])
                puesto = get_val(row, ["PUESTO", "CARGO"])
                if not nombre or not usuario:
                    continue

                clave_txt = str(clave).strip() if clave else f"{usuario}2025"
                rol_txt = str(rol).strip().lower() if rol else "usuario"
                if "gestor" in rol_txt or "admin" in rol_txt or "monitor" in rol_txt:
                    rol_txt = "admin"
                elif "usuario" in rol_txt or "user" in rol_txt:
                    rol_txt = "user"
                elif rol_txt not in {"admin", "user"}:
                    rol_txt = "user"

                entes_txt = str(entes).strip().upper() if entes else "TODOS"
                usuarios.append((
                    str(nombre).strip(),
                    str(usuario).strip(),
                    _hash_password(clave_txt),
                    rol_txt,
                    str(puesto).strip() if puesto else "",
                    entes_txt,
                ))

        if total == 0 and not usuarios:
            usuarios = [
                ("C.P. Miguel Ángel Roldán Peña", "miguel", _hash_password("miguel2025"), "user", "", "TODOS"),
                ("C.P. Cristina Rosas de la Cruz", "cristina", _hash_password("cristina2025"), "user", "", "TODOS"),
                ("C.P. Ángel Flores Licona", "angel", _hash_password("angel2025"), "user", "", "TODOS"),
                ("C.P. Juan José Blanco Sánchez", "juan", _hash_password("juan2025"), "user", "", "TODOS"),
                ("Ing. Omar Alfredo Castro Orozco", "omar", _hash_password("omar2025"), "user", "", "TODOS"),
            ]
            logger.warning("Usuarios base creados; cambie las claves por seguridad.")

        if total == 0 and usuarios:
            cur.executemany(
                "INSERT INTO usuarios (nombre, usuario, clave, rol, puesto, entes) VALUES (?, ?, ?, ?, ?, ?)",
                usuarios,
            )

        conn.commit()
        conn.close()

    # -------------------------------------------------------
    # Usuarios
    # -------------------------------------------------------
    def get_usuario(self, usuario: str, clave: str):
        if not usuario or not clave:
            return None
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre, usuario, clave, rol, entes
            FROM usuarios
            WHERE LOWER(usuario)=LOWER(?)
              AND activo=1
            LIMIT 1
        """, (usuario,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        if _hash_password(clave) != row["clave"]:
            return None
        entes = [e.strip().upper() for e in (row["entes"] or "").split(",") if e.strip()]
        return {
            "id": row["id"],
            "nombre": row["nombre"],
            "usuario": row["usuario"],
            "rol": row["rol"],
            "entes": entes or ["TODOS"],
        }

    def get_usuario_por_username(self, usuario: str):
        if not usuario:
            return None
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre, usuario, rol, entes
            FROM usuarios
            WHERE LOWER(usuario)=LOWER(?)
              AND activo=1
            LIMIT 1
        """, (usuario,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        entes = [e.strip().upper() for e in (row["entes"] or "").split(",") if e.strip()]
        return {
            "id": row["id"],
            "nombre": row["nombre"],
            "usuario": row["usuario"],
            "rol": row["rol"],
            "entes": entes or ["TODOS"],
        }

    # -------------------------------------------------------
    # Catálogos
    # -------------------------------------------------------
    def listar_entes(self) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT clave, nombre, tipo
            FROM entes
            WHERE activo=1
            ORDER BY nombre
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def _resolver_destinos_cursor(self, cur, ruta_destinos: List[str]) -> Tuple[List[str], Dict[str, str], List[str]]:
        entradas = [
            str(clave).strip()
            for clave in ruta_destinos
            if clave and str(clave).strip()
        ]
        if not entradas:
            return [], {}, []

        cur.execute("""
            SELECT clave, nombre
            FROM entes
            WHERE activo=1
        """)
        indice: Dict[str, str] = {}
        nombres: Dict[str, str] = {}
        for row in cur.fetchall():
            clave = (row["clave"] or "").strip()
            if not clave:
                continue
            nombre = (row["nombre"] or "").strip()
            nombres[clave] = nombre
            variantes = _variantes_ente(clave) + _variantes_ente(nombre)
            for variante in variantes:
                indice.setdefault(variante, clave)

        destinos: List[str] = []
        faltantes: List[str] = []
        for entrada in entradas:
            clave_resuelta = None
            for variante in _variantes_ente(entrada):
                clave_resuelta = indice.get(variante)
                if clave_resuelta:
                    break
            if clave_resuelta:
                destinos.append(clave_resuelta)
            else:
                faltantes.append(entrada)

        nombres_destinos = {
            clave: nombres.get(clave, "")
            for clave in set(destinos)
        }
        return destinos, nombres_destinos, faltantes

    def obtener_usuario_id(self, usuario: str) -> Optional[int]:
        if not usuario:
            return None
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id
            FROM usuarios
            WHERE LOWER(usuario)=LOWER(?)
        """, (usuario.strip(),))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return row["id"]

    def listar_usuarios(self, resguardante_id: Optional[int] = None) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        if resguardante_id:
            cur.execute("""
                SELECT usuario
                FROM usuarios
                WHERE id=?
            """, (resguardante_id,))
            row = cur.fetchone()
            if row and (row["usuario"] or "").lower() in {"luis", "odilia"}:
                resguardante_id = None

        if resguardante_id:
            cur.execute("""
                SELECT u.id, u.nombre, u.usuario, u.rol, u.puesto
                FROM usuarios u
                JOIN usuarios_personal up ON up.personal_id = u.id
                WHERE up.resguardante_id=?
                  AND u.activo=1
                  AND u.id != ?
                ORDER BY u.nombre
            """, (resguardante_id, resguardante_id))
        else:
            cur.execute("""
                SELECT id, nombre, usuario, rol, puesto
                FROM usuarios
                WHERE activo=1
                ORDER BY nombre
            """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def listar_auditores_por_usuario(self, usuario_id: int) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT nombre
            FROM usuarios
            WHERE id=? AND activo=1
        """, (usuario_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return []

        responsable_nombre = row["nombre"]
        cur.execute("""
            SELECT a.id, a.nombre
            FROM responsables r
            JOIN responsables_auditores ra ON ra.responsable_id = r.id
            JOIN auditores a ON a.id = ra.auditor_id
            WHERE r.nombre=? AND a.activo=1
            ORDER BY ra.orden, a.nombre
        """, (responsable_nombre,))
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def asegurar_auditor_usuario(self, usuario_id: int) -> Optional[int]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT nombre
            FROM usuarios
            WHERE id=? AND activo=1
        """, (usuario_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return None

        nombre_usuario = (row["nombre"] or "").strip()
        if not nombre_usuario:
            conn.close()
            return None

        cur.execute("""
            SELECT id, activo
            FROM auditores
            WHERE LOWER(TRIM(nombre)) = LOWER(TRIM(?))
            LIMIT 1
        """, (nombre_usuario,))
        auditor = cur.fetchone()
        if auditor:
            if int(auditor["activo"] or 0) != 1:
                cur.execute("UPDATE auditores SET activo=1 WHERE id=?", (auditor["id"],))
                conn.commit()
            auditor_id = int(auditor["id"])
            conn.close()
            return auditor_id

        cur.execute("""
            INSERT INTO auditores (nombre, activo)
            VALUES (?, 1)
        """, (nombre_usuario,))
        auditor_id = int(cur.lastrowid)
        conn.commit()
        conn.close()
        return auditor_id

    def listar_personal_resguardante(self, usuario_id: int) -> List[Dict]:
        auditores = self.listar_auditores_por_usuario(usuario_id)
        if not auditores:
            auditores = self.listar_auditores()
        return auditores

    def listar_resguardantes(self) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre
            FROM resguardantes
            WHERE activo=1
            ORDER BY nombre
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def _seed_vehiculos(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM vehiculos")
        if cur.fetchone()[0] > 0:
            conn.close()
            return

        vehiculos_base = [
            ("XVZ-360-C", "Versa Sense", "NISSAN"),
            ("XVZ-373-C", "Versa Sense", "NISSAN"),
            ("XVZ-351-C", "Versa", "NISSAN"),
            ("XVZ-357-C", "Versa Sense", "NISSAN"),
            ("XVZ-358-C", "Versa Sense", "NISSAN"),
            ("XVZ-346-C", "Versa", "NISSAN"),
            ("XVZ-370-C", "Versa Sense", "NISSAN"),
            ("XVZ-356-C", "Versa Sense", "NISSAN"),
            ("XTR-479-E", "Aveo", "CHEVROLET"),
            ("XVZ-359-C", "Versa Sense", "NISSAN"),
            ("XVZ-371-C", "Versa Sense", "NISSAN"),
            ("XVZ-349-C", "Versa", "NISSAN"),
            ("XVZ-353-C", "Versa", "NISSAN"),
            ("XXK-741-D", "Gol Sedán", "VOLKSWAGEN"),
            ("XC-8407-C", "NP 300", "NISSAN"),
            ("XVZ-335-C", "Aveo", "CHEVROLET"),
            ("XB-3501-D", "700", "RAM"),
            ("XB-3502-D", "700", "RAM"),
            ("XB-3503-D", "700", "RAM"),
        ]
        vehiculos = [
            (placa, modelo, marca, _categoria_por_placa(placa))
            for placa, modelo, marca in vehiculos_base
        ]
        cur.executemany("""
            INSERT INTO vehiculos (placa, modelo, marca, categoria, activo)
            VALUES (?, ?, ?, ?, 1)
        """, vehiculos)
        conn.commit()
        conn.close()

    def _seed_usuarios_vehiculos(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM usuarios_vehiculos")
        if cur.fetchone()[0] > 0:
            conn.close()
            return

        cur.execute("""
            SELECT id, usuario
            FROM usuarios
            WHERE activo=1
        """)
        usuarios = {row["usuario"].lower(): row["id"] for row in cur.fetchall()}

        cur.execute("""
            SELECT id, placa
            FROM vehiculos
            WHERE activo=1
        """)
        vehiculos = {row["placa"].upper(): row["id"] for row in cur.fetchall()}

        asignaciones = [
            ("cristina", ["XVZ-357-C", "XVZ-358-C", "XVZ-346-C"]),
            ("miguel", ["XVZ-360-C", "XVZ-373-C", "XVZ-351-C"]),
            ("omar", [
                "XXK-741-D",
                "XC-8407-C",
                "XVZ-335-C",
                "XB-3501-D",
                "XB-3502-D",
            ]),
            ("mike", ["XB-3503-D"]),
            ("ramos", []),
            ("angel", ["XVZ-353-C", "XVZ-370-C", "XVZ-356-C", "XTR-479-E"]),
            ("juan", ["XVZ-359-C", "XVZ-371-C", "XVZ-349-C"]),
        ]

        registros = []
        for usuario_key, placas in asignaciones:
            usuario_id = usuarios.get(usuario_key)
            if not usuario_id:
                continue
            for placa in placas:
                vehiculo_id = vehiculos.get(placa.upper())
                if vehiculo_id:
                    registros.append((usuario_id, vehiculo_id))

        if registros:
            cur.executemany("""
                INSERT OR IGNORE INTO usuarios_vehiculos (usuario_id, vehiculo_id)
                VALUES (?, ?)
            """, registros)

        conn.commit()
        conn.close()

    def _seed_responsables(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM responsables")
        if cur.fetchone()[0] > 0:
            conn.close()
            return

        responsables = [
            ("C.P. Miguel Ángel Roldán Peña",),
            ("C.P. Cristina Rosas de la Cruz",),
            ("C.P. Ángel Flores Licona",),
            ("C.P. Juan José Blanco Sánchez",),
            ("Ing. Omar Alfredo Castro Orozco",),
        ]
        cur.executemany("""
            INSERT INTO responsables (nombre, activo)
            VALUES (?, 1)
        """, responsables)
        conn.commit()
        conn.close()

    def _seed_resguardantes(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM resguardantes")
        if cur.fetchone()[0] > 0:
            conn.close()
            return

        resguardantes = [
            ("Luis Aguilar",),
            ("Monica Perez",),
            ("Daniel Ortiz",),
        ]
        cur.executemany("""
            INSERT INTO resguardantes (nombre, activo)
            VALUES (?, 1)
        """, resguardantes)
        conn.commit()
        conn.close()

    def _seed_auditores(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM auditores")
        if cur.fetchone()[0] > 0:
            conn.close()
            return

        auditores = [
            ("C.P. Yaneth Cruz George",),
            ("C.P. Julissa Karen Flores Pérez",),
            ("C.P. Margaret Michelle Pluma Meléndez",),
            ("C.P. Vanesa Angulo Ramírez",),
            ("C.P. Antonio Mastranzo Sánchez",),
            ("C.P. Gonzalo Flores Pérez",),
            ("C.P. Rubén Jesús Méndez Arámbula",),
            ("Lic. Liliana Bonilla Montiel",),
            ("C.P. Paola Rodríguez Sánchez",),
            ("C.P. David Yair Juárez Zainos",),
            ("C.P. Mayra Ortega Campech",),
            ("C.P. Omar Romero Flores",),
            ("Lic. Iván Xahuentitla Domínguez",),
            ("C.P. María Fernanda Vázquez Ramírez",),
            ("C.P. Luis Enrique Velázquez López",),
            ("C.P. Beatriz Netzahualcóyotl Nava",),
            ("C.P. Patricia Romano López",),
            ("C.P. Diana Angélica Mendoza Cortés",),
            ("C.P. Gloria Arévalo Gutiérrez",),
            ("C.P. Eliazar Nava Nava",),
            ("C.P. Edgar Daniel Ordoñez Salinas",),
            ("C.P. Melina Flores Peña",),
            ("C.P. Roberto Sánchez Espinoza",),
            ("C.P. Jonathan Islas Sosa",),
            ("C.P. Isael López Cervantes",),
            ("C.P. Aranza Sánchez Trejo",),
            ("Arq. Ramos Martín Quiebras Techalotzi",),
            ("Arq. Jesús Coca López",),
            ("Arq. Juan Eduardo López García",),
            ("Arq. José Miguel Ángel Morales Vásquez",),
            ("Arq. Guadalupe Carrillo Raya",),
            ("Ing. Sergio Grajeda Avendaño",),
            ("Ing. Ubaldo Cuapio Cuapio",),
            ("Arq. Sergio David Jiménez Cuahutepitzi",),
            ("Ing. Alfonso Luis Vázquez Barrera",),
            ("Arq. Arely López Breton",),
            ("Ing. Yessica Flores Flores",),
            ("Arq. Damaris Pérez Cárcamo",),
            ("Arq. Jesús Alberto Islas López",),
            ("Arq. José Luis Ramírez Hernández",),
            ("Arq. Monserrat Moncayo Gómez",),
            ("Ing. Omar Alejandro Limón",),
            ("Arq. Yazmín Zárate Pérez",),
        ]
        cur.executemany("""
            INSERT INTO auditores (nombre, activo)
            VALUES (?, 1)
        """, auditores)
        conn.commit()
        conn.close()

    def _seed_responsables_auditores(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM responsables_auditores")
        if cur.fetchone()[0] > 0:
            conn.close()
            return

        cur.execute("SELECT id, nombre FROM responsables WHERE activo=1")
        responsables = {row["nombre"]: row["id"] for row in cur.fetchall()}
        cur.execute("SELECT id, nombre FROM auditores WHERE activo=1")
        auditores = {row["nombre"]: row["id"] for row in cur.fetchall()}

        grupos = [
            ("C.P. Miguel Ángel Roldán Peña", [
                "C.P. Yaneth Cruz George",
                "C.P. Julissa Karen Flores Pérez",
                "C.P. Margaret Michelle Pluma Meléndez",
                "C.P. Vanesa Angulo Ramírez",
                "C.P. Antonio Mastranzo Sánchez",
                "C.P. Gonzalo Flores Pérez",
            ]),
            ("C.P. Cristina Rosas de la Cruz", [
                "Lic. Liliana Bonilla Montiel",
                "C.P. Paola Rodríguez Sánchez",
                "C.P. David Yair Juárez Zainos",
                "C.P. Mayra Ortega Campech",
                "C.P. Omar Romero Flores",
                "Lic. Iván Xahuentitla Domínguez",
                "C.P. María Fernanda Vázquez Ramírez",
            ]),
            ("C.P. Ángel Flores Licona", [
                "C.P. Luis Enrique Velázquez López",
                "C.P. Beatriz Netzahualcóyotl Nava",
                "C.P. Patricia Romano López",
                "C.P. Diana Angélica Mendoza Cortés",
                "C.P. Gloria Arévalo Gutiérrez",
                "C.P. Eliazar Nava Nava",
            ]),
            ("C.P. Juan José Blanco Sánchez", [
                "C.P. Edgar Daniel Ordoñez Salinas",
                "C.P. Melina Flores Peña",
                "C.P. Roberto Sánchez Espinoza",
                "C.P. Jonathan Islas Sosa",
                "C.P. Isael López Cervantes",
                "C.P. Aranza Sánchez Trejo",
                "C.P. Reynaldo Álvarez Teloxa",
            ]),
            ("Ing. Omar Alfredo Castro Orozco", [
                "Arq. Ramos Martín Quiebras Techalotzi",
                "Arq. Jesús Coca López",
                "Arq. Juan Eduardo López García",
                "Arq. José Miguel Ángel Morales Vásquez",
                "Arq. Guadalupe Carrillo Raya",
                "Ing. Sergio Grajeda Avendaño",
                "Ing. Ubaldo Cuapio Cuapio",
                "Arq. Sergio David Jiménez Cuahutepitzi",
                "Ing. Alfonso Luis Vázquez Barrera",
                "Arq. Arely López Breton",
                "Ing. Yessica Flores Flores",
                "Arq. Damaris Pérez Cárcamo",
                "Arq. Jesús Alberto Islas López",
                "Arq. José Luis Ramírez Hernández",
                "Arq. Monserrat Moncayo Gómez",
                "Ing. Omar Alejandro Limón",
                "Arq. Yazmín Zárate Pérez",
            ]),
        ]

        registros = []
        for responsable_nombre, lista_auditores in grupos:
            responsable_id = responsables.get(responsable_nombre)
            if not responsable_id:
                continue
            for orden, auditor_nombre in enumerate(lista_auditores, start=1):
                auditor_id = auditores.get(auditor_nombre)
                if auditor_id:
                    registros.append((responsable_id, auditor_id, orden))

        if registros:
            cur.executemany("""
                INSERT OR IGNORE INTO responsables_auditores (responsable_id, auditor_id, orden)
                VALUES (?, ?, ?)
            """, registros)

        conn.commit()
        conn.close()

    def _ensure_auditor_ruben_mendez(self) -> None:
        conn = self._connect()
        cur = conn.cursor()

        cur.execute("SELECT id, nombre FROM auditores")
        candidatos = [
            dict(row) for row in cur.fetchall()
            if _normalizar_clave(row["nombre"]).endswith(AUDITOR_RUBEN_MENDEZ_CLAVE)
        ]
        canonico = next(
            (row for row in candidatos if row["nombre"] == AUDITOR_RUBEN_MENDEZ_CANONICO),
            None,
        )

        if canonico:
            auditor_id = int(canonico["id"])
            cur.execute("UPDATE auditores SET activo=1 WHERE id=?", (auditor_id,))
        elif candidatos:
            auditor_id = int(candidatos[0]["id"])
            cur.execute("""
                UPDATE auditores
                SET nombre=?, activo=1
                WHERE id=?
            """, (AUDITOR_RUBEN_MENDEZ_CANONICO, auditor_id))
        else:
            cur.execute("""
                INSERT INTO auditores (nombre, activo)
                VALUES (?, 1)
            """, (AUDITOR_RUBEN_MENDEZ_CANONICO,))
            auditor_id = int(cur.lastrowid)

        for candidato in candidatos:
            candidato_id = int(candidato["id"])
            if candidato_id == auditor_id:
                continue
            cur.execute("""
                INSERT OR IGNORE INTO responsables_auditores (responsable_id, auditor_id, orden)
                SELECT responsable_id, ?, orden
                FROM responsables_auditores
                WHERE auditor_id=?
            """, (auditor_id, candidato_id))
            cur.execute("""
                INSERT OR IGNORE INTO movimientos_auditores (movimiento_id, auditor_id)
                SELECT movimiento_id, ?
                FROM movimientos_auditores
                WHERE auditor_id=?
            """, (auditor_id, candidato_id))
            cur.execute("DELETE FROM responsables_auditores WHERE auditor_id=?", (candidato_id,))
            cur.execute("DELETE FROM movimientos_auditores WHERE auditor_id=?", (candidato_id,))
            cur.execute("UPDATE auditores SET activo=0 WHERE id=?", (candidato_id,))

        responsables_objetivo = []
        cur.execute("""
            SELECT nombre
            FROM usuarios
            WHERE LOWER(usuario)=LOWER(?)
              AND activo=1
            LIMIT 1
        """, ("luis",))
        luis = cur.fetchone()
        if luis and luis["nombre"]:
            responsables_objetivo.append(luis["nombre"])

        for responsable_nombre in dict.fromkeys(responsables_objetivo):
            cur.execute("""
                INSERT OR IGNORE INTO responsables (nombre, activo)
                VALUES (?, 1)
            """, (responsable_nombre,))
            cur.execute("""
                UPDATE responsables
                SET activo=1
                WHERE nombre=?
            """, (responsable_nombre,))
            cur.execute("SELECT id FROM responsables WHERE nombre=?", (responsable_nombre,))
            responsable = cur.fetchone()
            if not responsable:
                continue

            responsable_id = int(responsable["id"])
            cur.execute("""
                SELECT 1
                FROM responsables_auditores
                WHERE responsable_id=? AND auditor_id=?
            """, (responsable_id, auditor_id))
            if cur.fetchone():
                continue

            cur.execute("""
                SELECT COALESCE(MAX(orden), 0) + 1
                FROM responsables_auditores
                WHERE responsable_id=?
            """, (responsable_id,))
            orden = int(cur.fetchone()[0])
            cur.execute("""
                INSERT INTO responsables_auditores (responsable_id, auditor_id, orden)
                VALUES (?, ?, ?)
            """, (responsable_id, auditor_id, orden))

        conn.commit()
        conn.close()

    def listar_vehiculos(self, usuario_id: Optional[int] = None) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        if usuario_id:
            cur.execute("""
                SELECT COUNT(*) AS total
                FROM usuarios_vehiculos
                WHERE usuario_id=?
            """, (usuario_id,))
            tiene_relacion = cur.fetchone()["total"] > 0
        else:
            tiene_relacion = False

        if usuario_id and tiene_relacion:
            cur.execute("""
                SELECT v.id, v.placa, v.modelo, v.marca, v.categoria,
                       (
                           SELECT u3.id
                           FROM usuarios_vehiculos uv3
                           JOIN usuarios u3 ON u3.id = uv3.usuario_id
                           WHERE uv3.vehiculo_id = v.id
                           ORDER BY u3.nombre
                           LIMIT 1
                       ) AS propietario_id,
                       COALESCE((
                           SELECT group_concat(nombre, ', ')
                           FROM (
                               SELECT u3.nombre AS nombre
                               FROM usuarios_vehiculos uv3
                               JOIN usuarios u3 ON u3.id = uv3.usuario_id
                               WHERE uv3.vehiculo_id = v.id
                               ORDER BY u3.nombre
                           )
                       ), '') AS propietarios_nombres
                FROM vehiculos v
                JOIN usuarios_vehiculos uv ON uv.vehiculo_id = v.id
                WHERE v.activo=1 AND uv.usuario_id=?
                ORDER BY v.placa
            """, (usuario_id,))
        else:
            cur.execute("""
                SELECT v.id, v.placa, v.modelo, v.marca, v.categoria,
                       (
                           SELECT u3.id
                           FROM usuarios_vehiculos uv3
                           JOIN usuarios u3 ON u3.id = uv3.usuario_id
                           WHERE uv3.vehiculo_id = v.id
                           ORDER BY u3.nombre
                           LIMIT 1
                       ) AS propietario_id,
                       COALESCE((
                           SELECT group_concat(nombre, ', ')
                           FROM (
                               SELECT u3.nombre AS nombre
                               FROM usuarios_vehiculos uv3
                               JOIN usuarios u3 ON u3.id = uv3.usuario_id
                               WHERE uv3.vehiculo_id = v.id
                               ORDER BY u3.nombre
                           )
                       ), '') AS propietarios_nombres
                FROM vehiculos v
                WHERE v.activo=1
                ORDER BY v.placa
            """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def listar_vehiculos_por_categoria(self, categoria: str) -> List[Dict]:
        categoria_txt = _normalizar_categoria_vehiculo(categoria)
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT v.id, v.placa, v.modelo, v.marca, v.categoria,
                   (
                       SELECT u3.id
                       FROM usuarios_vehiculos uv3
                       JOIN usuarios u3 ON u3.id = uv3.usuario_id
                       WHERE uv3.vehiculo_id = v.id
                       ORDER BY u3.nombre
                       LIMIT 1
                   ) AS propietario_id,
                   COALESCE((
                       SELECT group_concat(nombre, ', ')
                       FROM (
                           SELECT u3.nombre AS nombre
                           FROM usuarios_vehiculos uv3
                           JOIN usuarios u3 ON u3.id = uv3.usuario_id
                           WHERE uv3.vehiculo_id = v.id
                           ORDER BY u3.nombre
                       )
                   ), '') AS propietarios_nombres
            FROM vehiculos v
            WHERE v.activo=1
              AND v.categoria=?
            ORDER BY v.placa
        """, (categoria_txt,))
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def _obtener_resguardantes_vehiculo_cursor(self, cur: sqlite3.Cursor, vehiculo_id: int) -> Optional[Dict[str, Any]]:
        cur.execute("""
            SELECT u.id, u.nombre
            FROM usuarios_vehiculos uv
            JOIN usuarios u ON u.id = uv.usuario_id
            WHERE uv.vehiculo_id=?
            ORDER BY u.nombre
        """, (vehiculo_id,))
        rows = [dict(row) for row in cur.fetchall()]
        if not rows:
            return None
        return {
            "id": rows[0]["id"] if len(rows) == 1 else None,
            "nombre": ", ".join(row["nombre"] for row in rows if row["nombre"]),
        }

    @staticmethod
    def _aplicar_resguardante_asignado(movimiento: Dict) -> Dict:
        propietarios = (movimiento.get("propietarios_nombres") or "").strip()
        if propietarios:
            movimiento["resguardante_nombre"] = propietarios
            if "," in propietarios:
                movimiento["resguardante_id"] = None
        return movimiento

    def listar_vehiculos_con_propietarios(self) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT v.id, v.placa, v.modelo, v.marca, v.categoria,
                   MIN(u.id) AS propietario_id,
                   COALESCE(GROUP_CONCAT(u.nombre, ', '), '') AS propietarios_nombres
            FROM vehiculos v
            LEFT JOIN usuarios_vehiculos uv ON uv.vehiculo_id = v.id
            LEFT JOIN usuarios u ON u.id = uv.usuario_id
            WHERE v.activo=1
            GROUP BY v.id
            ORDER BY v.placa
        """)
        data = [dict(row) for row in cur.fetchall()]
        conn.close()
        return data

    def listar_vehiculos_disponibles_con_propietarios(
        self,
        fecha_iso: Optional[str] = None,
    ) -> List[Dict]:
        fecha_txt = _parse_date(fecha_iso) or _hoy_iso()
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT v.id, v.placa, v.modelo, v.marca, v.categoria,
                   COALESCE(GROUP_CONCAT(u.nombre, ', '), '') AS propietarios_nombres
            FROM vehiculos v
            LEFT JOIN usuarios_vehiculos uv ON uv.vehiculo_id = v.id
            LEFT JOIN usuarios u ON u.id = uv.usuario_id
            WHERE v.activo=1
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos m
                  WHERE m.vehiculo_id = v.id
                    AND m.fecha_solicitud = ?
                    AND NOT EXISTS (
                        SELECT 1
                        FROM movimientos_eventos me
                        WHERE me.movimiento_id = m.id
                          AND me.evento = 'RECHAZADO'
                    )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM prestamos_vehiculos p
                  WHERE p.vehiculo_id = v.id
                    AND p.estado = 'VALIDADO'
                    AND (',' || COALESCE(p.fechas_solicitadas, '') || ',') LIKE '%,' || ? || ',%'
              )
            GROUP BY v.id
            ORDER BY v.placa
        """, (fecha_txt, fecha_txt))
        data = [dict(row) for row in cur.fetchall()]
        conn.close()
        return data

    def listar_vehiculos_disponibles(self, fecha_iso: Optional[str] = None) -> List[Dict]:
        fecha_txt = _parse_date(fecha_iso) or _hoy_iso()
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT v.id, v.placa, v.modelo, v.marca, v.categoria
            FROM vehiculos v
            WHERE v.activo=1
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos m
                  WHERE m.vehiculo_id = v.id
                    AND m.fecha_solicitud = ?
                    AND NOT EXISTS (
                        SELECT 1
                        FROM movimientos_eventos me
                        WHERE me.movimiento_id = m.id
                          AND me.evento = 'RECHAZADO'
                    )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM prestamos_vehiculos p
                  WHERE p.vehiculo_id = v.id
                    AND p.estado = 'VALIDADO'
                    AND (',' || COALESCE(p.fechas_solicitadas, '') || ',') LIKE '%,' || ? || ',%'
              )
            ORDER BY v.placa
        """, (fecha_txt, fecha_txt))
        data = [dict(row) for row in cur.fetchall()]
        conn.close()
        return data

    def listar_vehiculos_prestables(
        self,
        solicitante_id: int,
        fecha_iso: Optional[str] = None,
    ) -> List[Dict]:
        fecha_txt = _parse_date(fecha_iso) or _hoy_iso()
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT v.id, v.placa, v.modelo, v.marca, v.categoria,
                   u.id AS propietario_id, u.nombre AS propietario_nombre
            FROM vehiculos v
            JOIN usuarios_vehiculos uv ON uv.vehiculo_id = v.id
            JOIN usuarios u ON u.id = uv.usuario_id
            WHERE v.activo=1
              AND u.activo=1
              AND uv.usuario_id != ?
              AND NOT (
                  UPPER(v.placa) = 'XVZ-353-C'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM usuarios us
                      WHERE us.id = ?
                        AND LOWER(us.usuario) = 'angel'
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos m
                  WHERE m.vehiculo_id = v.id
                    AND m.fecha_solicitud = ?
                    AND NOT EXISTS (
                        SELECT 1
                        FROM movimientos_eventos me
                        WHERE me.movimiento_id = m.id
                          AND me.evento = 'RECHAZADO'
                    )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM prestamos_vehiculos p
                  WHERE p.vehiculo_id = v.id
                    AND p.estado IN ('PENDIENTE', 'VALIDADO')
                    AND (
                        (',' || COALESCE(p.fechas_solicitadas, '') || ',') LIKE '%,' || ? || ',%'
                        OR (
                            TRIM(COALESCE(p.fechas_solicitadas, '')) = ''
                            AND p.fecha_solicitud = ?
                        )
                    )
              )
            ORDER BY u.nombre, v.placa
        """, (solicitante_id, solicitante_id, fecha_txt, fecha_txt, fecha_txt))
        data = [dict(row) for row in cur.fetchall()]
        conn.close()
        return data

    def contar_vehiculos_disponibles(self, fecha_iso: Optional[str] = None) -> Tuple[int, int]:
        fecha_txt = _parse_date(fecha_iso) or _hoy_iso()
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) AS total
            FROM vehiculos
            WHERE activo=1
        """)
        total = cur.fetchone()["total"]
        cur.execute("""
            SELECT COUNT(DISTINCT vehiculo_id) AS en_uso
            FROM (
                SELECT m.vehiculo_id AS vehiculo_id
                FROM movimientos m
                WHERE m.vehiculo_id IS NOT NULL
                  AND m.fecha_solicitud = ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM movimientos_eventos me
                      WHERE me.movimiento_id = m.id
                        AND me.evento = 'RECHAZADO'
                  )
                UNION
                SELECT p.vehiculo_id AS vehiculo_id
                FROM prestamos_vehiculos p
                WHERE p.vehiculo_id IS NOT NULL
                  AND p.estado = 'VALIDADO'
                  AND (',' || COALESCE(p.fechas_solicitadas, '') || ',') LIKE '%,' || ? || ',%'
            )
        """, (fecha_txt, fecha_txt))
        en_uso = cur.fetchone()["en_uso"]
        conn.close()
        disponibles = max(total - en_uso, 0)
        return total, disponibles

    def obtener_vehiculos_ocupados(self, fecha_iso: Optional[str] = None) -> set:
        fecha_txt = _parse_date(fecha_iso) or _hoy_iso()
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT vehiculo_id
            FROM (
                SELECT m.vehiculo_id AS vehiculo_id
                FROM movimientos m
                WHERE m.vehiculo_id IS NOT NULL
                  AND m.fecha_solicitud = ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM movimientos_eventos me
                      WHERE me.movimiento_id = m.id
                        AND me.evento = 'RECHAZADO'
                  )
                UNION
                SELECT p.vehiculo_id AS vehiculo_id
                FROM prestamos_vehiculos p
                WHERE p.vehiculo_id IS NOT NULL
                  AND p.estado = 'VALIDADO'
                  AND (',' || COALESCE(p.fechas_solicitadas, '') || ',') LIKE '%,' || ? || ',%'
            )
        """, (fecha_txt, fecha_txt))
        ocupados = {row["vehiculo_id"] for row in cur.fetchall() if row["vehiculo_id"]}
        conn.close()
        return ocupados

    def _obtener_auditores_ocupados_cursor(
        self,
        cur,
        fecha_txt: str,
        movimiento_excluido: Optional[int] = None,
        prestamo_excluido: Optional[int] = None,
    ) -> set:
        cur.execute("""
            SELECT DISTINCT a.id
            FROM movimientos m
            JOIN auditores a
              ON LOWER(TRIM(a.nombre)) = LOWER(TRIM(COALESCE(m.responsable_vehiculo, '')))
            WHERE m.fecha_solicitud = ?
              AND (? IS NULL OR m.id != ?)
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos_eventos me
                  WHERE me.movimiento_id = m.id
                    AND me.evento = 'RECHAZADO'
              )
        """, (fecha_txt, movimiento_excluido, movimiento_excluido))
        ocupados = {row["id"] for row in cur.fetchall() if row["id"]}

        cur.execute("""
            SELECT DISTINCT a.id
            FROM movimientos m
            JOIN movimientos_auditores ma ON ma.movimiento_id = m.id
            JOIN auditores a ON a.id = ma.auditor_id
            WHERE m.fecha_solicitud = ?
              AND (? IS NULL OR m.id != ?)
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos_eventos me
                  WHERE me.movimiento_id = m.id
                    AND me.evento = 'RECHAZADO'
              )
        """, (fecha_txt, movimiento_excluido, movimiento_excluido))
        ocupados.update({row["id"] for row in cur.fetchall() if row["id"]})

        cur.execute("""
            SELECT DISTINCT a.id
            FROM prestamos_vehiculos p
            JOIN auditores a
              ON LOWER(TRIM(a.nombre)) = LOWER(TRIM(COALESCE(p.responsable_nombre, '')))
            WHERE p.estado IN ('PENDIENTE', 'VALIDADO')
              AND (? IS NULL OR p.id != ?)
              AND (
                (
                  TRIM(COALESCE(p.fechas_solicitadas, '')) != ''
                  AND (',' || p.fechas_solicitadas || ',') LIKE '%,' || ? || ',%'
                )
                OR (
                  TRIM(COALESCE(p.fechas_solicitadas, '')) = ''
                  AND p.fecha_solicitud = ?
                )
              )
        """, (prestamo_excluido, prestamo_excluido, fecha_txt, fecha_txt))
        ocupados.update({row["id"] for row in cur.fetchall() if row["id"]})

        cur.execute("""
            SELECT p.pasajeros_ids
            FROM prestamos_vehiculos p
            WHERE p.estado IN ('PENDIENTE', 'VALIDADO')
              AND (? IS NULL OR p.id != ?)
              AND (
                (
                  TRIM(COALESCE(p.fechas_solicitadas, '')) != ''
                  AND (',' || p.fechas_solicitadas || ',') LIKE '%,' || ? || ',%'
                )
                OR (
                  TRIM(COALESCE(p.fechas_solicitadas, '')) = ''
                  AND p.fecha_solicitud = ?
                )
              )
        """, (prestamo_excluido, prestamo_excluido, fecha_txt, fecha_txt))
        pasajeros_por_prestamo = cur.fetchall()
        if pasajeros_por_prestamo:
            cur.execute("SELECT id FROM auditores WHERE activo=1")
            auditores_ids = {row["id"] for row in cur.fetchall()}
            for row in pasajeros_por_prestamo:
                pasajeros_txt = row["pasajeros_ids"] or ""
                for raw_id in pasajeros_txt.split(","):
                    raw_id = raw_id.strip()
                    if not raw_id.isdigit():
                        continue
                    pid = int(raw_id)
                    if pid in auditores_ids:
                        ocupados.add(pid)
        return ocupados

    def _usuario_responsable_ocupado_cursor(
        self,
        cur,
        usuario_id: int,
        usuario_nombre: str,
        fecha_txt: str,
    ) -> bool:
        cur.execute("""
            SELECT 1
            FROM movimientos m
            WHERE m.responsable_id = ?
              AND LOWER(TRIM(COALESCE(m.responsable_vehiculo, ''))) = LOWER(TRIM(?))
              AND m.fecha_solicitud = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos_eventos me
                  WHERE me.movimiento_id = m.id
                    AND me.evento = 'RECHAZADO'
              )
            LIMIT 1
        """, (usuario_id, usuario_nombre, fecha_txt))
        if cur.fetchone():
            return True
        cur.execute("""
            SELECT 1
            FROM prestamos_vehiculos p
            WHERE p.responsable_id = ?
              AND LOWER(TRIM(COALESCE(p.responsable_nombre, ''))) = LOWER(TRIM(?))
              AND p.estado IN ('PENDIENTE', 'VALIDADO')
              AND (
                (',' || COALESCE(p.fechas_solicitadas, '') || ',') LIKE '%,' || ? || ',%'
                OR p.fecha_solicitud = ?
              )
            LIMIT 1
        """, (usuario_id, usuario_nombre, fecha_txt, fecha_txt))
        return bool(cur.fetchone())

    def _formatear_ruta_destino_con_entes_cursor(self, cur, ruta_destino: Optional[str]) -> str:
        destinos = _split_ruta_destino(ruta_destino)
        if not destinos:
            return (ruta_destino or "").strip()
        cur.execute("""
            SELECT clave, nombre
            FROM entes
            WHERE activo=1
        """)
        mapa_entes = {
            str(row["clave"]).upper(): (row["nombre"] or "").strip()
            for row in cur.fetchall()
            if row["clave"]
        }
        destinos_legibles: List[str] = []
        for destino in destinos:
            destino_upper = destino.strip().upper()
            clave_normalizada = _clave_ente_canonica(destino)
            clave = None
            if destino_upper in mapa_entes:
                clave = destino_upper
            elif clave_normalizada in mapa_entes:
                clave = clave_normalizada
            if clave:
                if clave == "CCLET":
                    destinos_legibles.append("CCLET")
                else:
                    destinos_legibles.append(mapa_entes.get(clave) or destino.replace("_", " "))
            else:
                destinos_legibles.append(destino.replace("_", " "))
        return " -> ".join(destinos_legibles)

    def obtener_auditores_ocupados(self, fecha_iso: Optional[str] = None) -> set:
        fecha_txt = _parse_date(fecha_iso) or _hoy_iso()
        conn = self._connect()
        cur = conn.cursor()
        ocupados = self._obtener_auditores_ocupados_cursor(cur, fecha_txt)
        conn.close()
        return ocupados

    def obtener_usuarios_responsables_ocupados(self, fecha_iso: Optional[str] = None) -> set:
        fecha_txt = _parse_date(fecha_iso) or _hoy_iso()
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT u.id
            FROM movimientos m
            JOIN usuarios u
              ON u.id = m.responsable_id
             AND LOWER(TRIM(u.nombre)) = LOWER(TRIM(COALESCE(m.responsable_vehiculo, '')))
            WHERE m.fecha_solicitud = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos_eventos me
                  WHERE me.movimiento_id = m.id
                    AND me.evento = 'RECHAZADO'
              )
        """, (fecha_txt,))
        ocupados = {row["id"] for row in cur.fetchall() if row["id"]}

        cur.execute("""
            SELECT DISTINCT u.id
            FROM prestamos_vehiculos p
            JOIN usuarios u
              ON u.id = p.responsable_id
             AND LOWER(TRIM(u.nombre)) = LOWER(TRIM(COALESCE(p.responsable_nombre, '')))
            WHERE p.estado IN ('PENDIENTE', 'VALIDADO')
              AND (
                (',' || COALESCE(p.fechas_solicitadas, '') || ',') LIKE '%,' || ? || ',%'
                OR p.fecha_solicitud = ?
              )
        """, (fecha_txt, fecha_txt))
        ocupados.update({row["id"] for row in cur.fetchall() if row["id"]})
        conn.close()
        return ocupados

    def solicitar_prestamo(
        self,
        solicitante_id: int,
        propietario_id: int,
        vehiculo_id: int,
        responsable_usuario_id: Optional[int],
        responsable_tipo: str,
        no_pasajeros: int,
        pasajeros_ids: List[int],
        fechas_solicitadas: List[str],
        ruta_destinos: List[str],
        motivo_salida: str,
        notas: Optional[str] = None,
        fecha_solicitud: Optional[str] = None,
    ) -> Tuple[bool, Union[str, Dict[str, Any]]]:
        if solicitante_id == propietario_id:
            return False, "No puede solicitar un prestamo de su propio vehiculo."

        requeridos = [
            ("vehiculo", vehiculo_id),
            ("responsable", responsable_usuario_id),
            ("pasajeros", no_pasajeros),
            ("fechas", fechas_solicitadas),
            ("ruta", ruta_destinos),
            ("motivo", motivo_salida),
        ]
        no_pasajeros_int = int(no_pasajeros)
        if no_pasajeros_int > 0:
            requeridos.append(("nombres_pasajeros", pasajeros_ids))
        for clave, valor in requeridos:
            if valor is None or (isinstance(valor, str) and not valor.strip()):
                return False, f"Falta el dato de {clave}."
            if isinstance(valor, list) and not valor:
                return False, f"Falta el dato de {clave}."
        if motivo_salida not in MOTIVOS_SALIDA_VALIDOS:
            return False, "Motivo de salida no valido."

        fechas_limpias = []
        for fecha in fechas_solicitadas:
            fecha_txt = _parse_date(fecha)
            if not fecha_txt:
                return False, "Formato de fecha no valido para el prestamo."
            try:
                fechas_limpias.append(datetime.strptime(fecha_txt, "%Y-%m-%d").date())
            except ValueError:
                return False, "Formato de fecha no valido para el prestamo."
        if not fechas_limpias:
            return False, "Falta seleccionar al menos una fecha."
        if len(fechas_limpias) > 1:
            return False, "Solo se permite solicitar un dia por prestamo."
        min_fecha, max_fecha = _limites_dias_habiles()
        for fecha in fechas_limpias:
            if fecha.weekday() >= 5 or fecha < min_fecha or fecha > max_fecha:
                return False, "Las fechas deben estar dentro de los dias habiles permitidos."
        fechas_txt = ",".join([fecha.isoformat() for fecha in sorted(set(fechas_limpias))])

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT 1
            FROM usuarios_vehiculos
            WHERE usuario_id=? AND vehiculo_id=?
        """, (propietario_id, vehiculo_id))
        if not cur.fetchone():
            conn.close()
            return False, "El vehiculo no esta asignado al usuario seleccionado."

        cur.execute("""
            SELECT 1
            FROM usuarios_vehiculos
            WHERE usuario_id=? AND vehiculo_id=?
        """, (solicitante_id, vehiculo_id))
        if cur.fetchone():
            conn.close()
            return False, "El vehiculo ya esta asignado al solicitante."

        cur.execute("""
            SELECT v.placa, us.usuario AS solicitante_usuario
            FROM vehiculos v
            JOIN usuarios us ON us.id=?
            WHERE v.id=?
            LIMIT 1
        """, (solicitante_id, vehiculo_id))
        vehiculo_solicitante = cur.fetchone()
        if (
            vehiculo_solicitante
            and (vehiculo_solicitante["placa"] or "").strip().upper() in PLACAS_SOLO_PROPIETARIO
            and (vehiculo_solicitante["solicitante_usuario"] or "").strip().lower() != "angel"
        ):
            conn.close()
            return False, "El vehiculo solo esta disponible para Angel."

        fecha_reserva = fechas_limpias[0].isoformat()
        cur.execute("""
            SELECT 1
            FROM movimientos m
            WHERE m.vehiculo_id=?
              AND m.fecha_solicitud=?
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos_eventos me
                  WHERE me.movimiento_id = m.id
                    AND me.evento = 'RECHAZADO'
              )
            LIMIT 1
        """, (vehiculo_id, fecha_reserva))
        if cur.fetchone():
            conn.close()
            return False, "El vehiculo ya esta asignado para esa fecha."

        cur.execute("""
            SELECT fecha_solicitud, fechas_solicitadas, estado
            FROM prestamos_vehiculos
            WHERE vehiculo_id=?
              AND estado IN ('PENDIENTE', 'VALIDADO')
        """, (vehiculo_id,))
        for row in cur.fetchall():
            fechas_existentes_txt = row["fechas_solicitadas"] or ""
            fechas_reserva = {
                fecha.strip()
                for fecha in fechas_existentes_txt.split(",")
                if fecha.strip()
            }
            fecha_registro = _parse_date(row["fecha_solicitud"])
            if not fechas_reserva and fecha_registro:
                fechas_reserva.add(fecha_registro)
            if fecha_reserva in fechas_reserva:
                conn.close()
                estado = (row["estado"] or "").upper()
                if estado == "VALIDADO":
                    return False, "El vehiculo ya tiene un prestamo validado para esa fecha."
                return False, "El vehiculo ya tiene un prestamo pendiente para esa fecha."

        responsable_info = self._obtener_responsable(cur, responsable_tipo, responsable_usuario_id)
        if not responsable_info:
            conn.close()
            return False, "Responsable no encontrado."
        responsable_nombre, responsable_id_db = responsable_info
        if responsable_tipo == "usuario" and self._usuario_responsable_ocupado_cursor(
            cur,
            int(responsable_usuario_id),
            responsable_nombre,
            fecha_reserva,
        ):
            conn.close()
            return False, "El responsable ya esta asignado como piloto para esa fecha."

        pasajeros_ids = [int(pid) for pid in pasajeros_ids if pid]
        if responsable_tipo == "auditor" and responsable_usuario_id in pasajeros_ids:
            pasajeros_ids = [pid for pid in pasajeros_ids if pid != responsable_usuario_id]
            no_pasajeros_int = max(no_pasajeros_int - 1, 0)
        if no_pasajeros_int == 0:
            pasajeros_ids = []
        if pasajeros_ids:
            if len(pasajeros_ids) != len(set(pasajeros_ids)):
                conn.close()
                return False, "Pasajeros duplicados en la seleccion."
            placeholders = ",".join(["?"] * len(pasajeros_ids))
            cur.execute(f"""
                SELECT id
                FROM auditores
                WHERE id IN ({placeholders}) AND activo=1
            """, pasajeros_ids)
            pasajeros = cur.fetchall()
            if len(pasajeros) != len(set(pasajeros_ids)):
                conn.close()
                return False, "Pasajeros no encontrados."
        if no_pasajeros_int != len(pasajeros_ids):
            conn.close()
            return False, "El numero de pasajeros no coincide."
        ocupados_auditores = self._obtener_auditores_ocupados_cursor(cur, fecha_reserva)
        if responsable_tipo == "auditor" and responsable_usuario_id in ocupados_auditores:
            conn.close()
            return False, "El responsable ya esta asignado como piloto o pasajero para esa fecha."
        for pasajero_id in pasajeros_ids:
            if pasajero_id in ocupados_auditores:
                conn.close()
                return False, "Uno o mas pasajeros ya estan asignados como piloto o pasajero para esa fecha."

        destinos, entes_validos_map, destinos_faltantes = self._resolver_destinos_cursor(cur, ruta_destinos)
        if not destinos:
            conn.close()
            return False, "Falta el dato de ruta."
        if destinos_faltantes:
            conn.close()
            return False, "Destinos no encontrados en catalogo de entes."
        destinos_legibles = [
            "CCLET" if clave == "CCLET" else (entes_validos_map.get(clave) or clave.replace("_", " "))
            for clave in destinos
        ]

        cur.execute("""
            SELECT COUNT(*) AS total
            FROM prestamos_vehiculos
            WHERE solicitante_id=? AND propietario_id=? AND vehiculo_id=? AND estado='PENDIENTE'
        """, (solicitante_id, propietario_id, vehiculo_id))
        if cur.fetchone()["total"] > 0:
            conn.close()
            return False, "Ya existe una solicitud pendiente para este vehiculo."

        cur.execute("""
            INSERT INTO prestamos_vehiculos (
                solicitante_id, propietario_id, vehiculo_id, fecha_solicitud, estado, notas,
                responsable_id, responsable_nombre, no_pasajeros, pasajeros_ids, ruta_destino, motivo_salida, fechas_solicitadas
            ) VALUES (?, ?, ?, ?, 'PENDIENTE', ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            solicitante_id,
            propietario_id,
            vehiculo_id,
            _parse_date(fecha_solicitud) or _hoy_iso(),
            notas.strip() if notas else None,
            responsable_id_db,
            responsable_nombre,
            int(no_pasajeros_int),
            ",".join([str(pid) for pid in pasajeros_ids]) if pasajeros_ids else None,
            " -> ".join(destinos_legibles),
            motivo_salida.strip(),
            fechas_txt,
        ))
        prestamo_id = cur.lastrowid
        conn.commit()
        conn.close()
        return True, {
            "prestamo_id": prestamo_id,
            "mensaje": "Solicitud de prestamo registrada.",
        }

    def listar_responsables(self) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre
            FROM responsables
            WHERE activo=1
            ORDER BY nombre
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def listar_auditores(self) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre
            FROM auditores
            WHERE activo=1
            ORDER BY nombre
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def listar_usuarios_resguardo(self) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre, usuario
            FROM usuarios
            WHERE activo=1
              AND LOWER(COALESCE(rol, '')) != 'monitor'
            ORDER BY nombre
        """)
        data = []
        for row in cur.fetchall():
            item = dict(row)
            item["categoria_resguardo"] = _categoria_resguardo_usuario(item.get("usuario"))
            data.append(item)
        conn.close()
        return data

    def crear_vehiculo(
        self,
        placa: str,
        marca: str,
        modelo: str,
        categoria: Optional[str] = None,
    ) -> Tuple[bool, str]:
        if not placa or not marca or not modelo:
            return False, "Faltan datos requeridos."
        categoria_txt = _normalizar_categoria_vehiculo(categoria)
        conn = self._connect()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO vehiculos (placa, modelo, marca, categoria, activo)
                VALUES (?, ?, ?, ?, 1)
            """, (
                placa.strip().upper(),
                modelo.strip(),
                marca.strip().upper(),
                categoria_txt,
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return False, "La placa ya existe."
        conn.close()
        return True, "Vehiculo registrado correctamente."

    def reasignar_vehiculo(
        self,
        vehiculo_id: int,
        usuario_destino_id: int,
        categoria: Optional[str],
        modo: str = "mover",
    ) -> Tuple[bool, str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, categoria
            FROM vehiculos
            WHERE id=? AND activo=1
        """, (vehiculo_id,))
        vehiculo = cur.fetchone()
        if not vehiculo:
            conn.close()
            return False, "Vehiculo no encontrado."
        categoria_vehiculo = _normalizar_categoria_vehiculo(vehiculo["categoria"])
        categoria_solicitada = _normalizar_categoria_vehiculo(categoria)
        if categoria and categoria_solicitada != categoria_vehiculo:
            conn.close()
            return False, "La categoria del vehiculo no puede cambiarse en la reasignacion."

        cur.execute("""
            SELECT id, usuario
            FROM usuarios
            WHERE id=? AND activo=1
              AND LOWER(COALESCE(rol, '')) != 'monitor'
        """, (usuario_destino_id,))
        usuario_destino = cur.fetchone()
        if not usuario_destino:
            conn.close()
            return False, "Resguardante no encontrado."
        categoria_usuario = _categoria_resguardo_usuario(usuario_destino["usuario"])
        if categoria_usuario != categoria_vehiculo:
            conn.close()
            return False, "El resguardante no corresponde a la categoria del vehiculo."

        cur.execute("DELETE FROM usuarios_vehiculos WHERE vehiculo_id=?", (vehiculo_id,))

        cur.execute("""
            INSERT OR IGNORE INTO usuarios_vehiculos (usuario_id, vehiculo_id)
            VALUES (?, ?)
        """, (usuario_destino_id, vehiculo_id))
        cur.execute("""
            UPDATE vehiculos
            SET categoria=?
            WHERE id=?
        """, (categoria_vehiculo, vehiculo_id))
        conn.commit()
        conn.close()

        return True, "Vehiculo reasignado correctamente."

    # -------------------------------------------------------
    # Movimientos
    # -------------------------------------------------------
    def _obtener_responsable(self, cur, responsable_tipo: str, responsable_id: Optional[int]) -> Optional[Tuple[str, Optional[int]]]:
        if responsable_id is None:
            return None
        if responsable_tipo == "auditor":
            cur.execute("""
                SELECT id, nombre
                FROM auditores
                WHERE id=? AND activo=1
            """, (responsable_id,))
            row = cur.fetchone()
            if not row:
                return None
            return row["nombre"], row["id"]
        cur.execute("""
            SELECT id, nombre
            FROM usuarios
            WHERE id=? AND activo=1
        """, (responsable_id,))
        row = cur.fetchone()
        if not row:
            return None
        return row["nombre"], row["id"]

    def crear_movimiento(
        self,
        usuario_id: int,
        ente_clave: str,
        cantidad: int,
        receptor_nombre: str,
        firma_recepcion: str,
        observaciones: str,
        resguardante_id: Optional[int],
        vehiculo_id: int,
        responsable_usuario_id: Optional[int],
        responsable_tipo: str,
        no_pasajeros: int,
        pasajeros_ids: List[int],
        ruta_destinos: List[str],
        motivo_salida: str,
        fecha_solicitud: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        requeridos = [
            ("vehiculo", vehiculo_id),
            ("responsable", responsable_usuario_id),
            ("pasajeros", no_pasajeros),
            ("ruta", ruta_destinos),
            ("motivo", motivo_salida),
        ]
        no_pasajeros_int = int(no_pasajeros)
        if no_pasajeros_int > 0:
            requeridos.append(("nombres_pasajeros", pasajeros_ids))
        for clave, valor in requeridos:
            if valor is None or (isinstance(valor, str) and not valor.strip()):
                return False, {"mensaje": f"Falta el dato de {clave}."}
            if isinstance(valor, list) and not valor:
                return False, {"mensaje": f"Falta el dato de {clave}."}
        if motivo_salida not in MOTIVOS_SALIDA_VALIDOS:
            return False, {"mensaje": "Motivo de salida no valido."}

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id
            FROM usuarios
            WHERE id=? AND activo=1
        """, (usuario_id,))
        usuario = cur.fetchone()
        if not usuario:
            conn.close()
            return False, {"mensaje": "Usuario no encontrado."}

        cur.execute("""
            SELECT id, placa, modelo, marca, categoria
            FROM vehiculos
            WHERE id=? AND activo=1
        """, (vehiculo_id,))
        vehiculo = cur.fetchone()
        if not vehiculo:
            conn.close()
            return False, {"mensaje": "Vehiculo no encontrado."}

        resguardante = self._obtener_resguardantes_vehiculo_cursor(cur, int(vehiculo_id))
        if not resguardante:
            conn.close()
            return False, {"mensaje": "La unidad no tiene resguardante asignado."}

        fecha_solicitud = _parse_date(fecha_solicitud) or _hoy_iso()
        cur.execute("""
            SELECT 1
            FROM movimientos m
            WHERE m.vehiculo_id=?
              AND m.fecha_solicitud=?
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos_eventos me
                  WHERE me.movimiento_id = m.id
                    AND me.evento = 'RECHAZADO'
              )
            LIMIT 1
        """, (vehiculo_id, fecha_solicitud))
        if cur.fetchone():
            conn.close()
            return False, {"mensaje": "La unidad ya esta asignada para esa fecha."}

        responsable_info = self._obtener_responsable(cur, responsable_tipo, responsable_usuario_id)
        if not responsable_info:
            conn.close()
            return False, {"mensaje": "Responsable no encontrado."}
        responsable_nombre, responsable_id_db = responsable_info
        if responsable_tipo == "usuario" and self._usuario_responsable_ocupado_cursor(
            cur,
            int(responsable_usuario_id),
            responsable_nombre,
            fecha_solicitud,
        ):
            conn.close()
            return False, {"mensaje": "El responsable ya esta asignado como piloto para esa fecha."}

        pasajeros_ids = [int(pid) for pid in pasajeros_ids if pid]
        if responsable_tipo == "auditor" and responsable_usuario_id in pasajeros_ids:
            pasajeros_ids = [pid for pid in pasajeros_ids if pid != responsable_usuario_id]
            no_pasajeros_int = max(no_pasajeros_int - 1, 0)
        if no_pasajeros_int == 0:
            pasajeros_ids = []
        if pasajeros_ids:
            if len(pasajeros_ids) != len(set(pasajeros_ids)):
                conn.close()
                return False, {"mensaje": "Pasajeros duplicados en la seleccion."}
            placeholders = ",".join(["?"] * len(pasajeros_ids))
            cur.execute(f"""
                SELECT id, nombre
                FROM auditores
                WHERE id IN ({placeholders}) AND activo=1
                ORDER BY nombre
            """, pasajeros_ids)
            pasajeros = cur.fetchall()
            if len(pasajeros) != len(set(pasajeros_ids)):
                conn.close()
                return False, {"mensaje": "Pasajeros no encontrados."}
        if no_pasajeros_int != len(pasajeros_ids):
            conn.close()
            return False, {"mensaje": "El numero de pasajeros no coincide."}
        ocupados_auditores = self._obtener_auditores_ocupados_cursor(cur, fecha_solicitud)
        if responsable_tipo == "auditor" and responsable_usuario_id in ocupados_auditores:
            conn.close()
            return False, {"mensaje": "El responsable ya esta asignado como piloto o pasajero para esa fecha."}
        for pasajero_id in pasajeros_ids:
            if pasajero_id in ocupados_auditores:
                conn.close()
                return False, {"mensaje": "Uno o mas pasajeros ya estan asignados como piloto o pasajero para esa fecha."}

        destinos, _, destinos_faltantes = self._resolver_destinos_cursor(cur, ruta_destinos)
        if not destinos:
            conn.close()
            return False, {"mensaje": "Falta el dato de ruta."}
        if destinos_faltantes:
            conn.close()
            return False, {"mensaje": "Destinos no encontrados en catalogo de entes."}
        folio = None
        receptor_nombre = receptor_nombre.strip() or resguardante["nombre"]
        ente_clave = destinos[0]

        max_intentos = 3
        for intento in range(max_intentos):
            folio = self._generar_folio(cur)
            try:
                cur.execute("""
                    INSERT INTO movimientos (
                        folio, usuario_id, ente_clave, fecha_solicitud,
                        cantidad, receptor_nombre, firma_recepcion, observaciones,
                        resguardante_nombre, resguardante_id, placa_unidad, marca, modelo,
                        responsable_vehiculo, vehiculo_id, responsable_id,
                        no_pasajeros, ruta_destino, motivo_salida
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    folio,
                    usuario_id,
                    ente_clave,
                    fecha_solicitud,
                    int(cantidad),
                    receptor_nombre.strip(),
                    firma_recepcion.strip() if firma_recepcion else None,
                    observaciones.strip() if observaciones else None,
                    resguardante["nombre"],
                    resguardante["id"],
                    vehiculo["placa"],
                    vehiculo["marca"],
                    vehiculo["modelo"],
                    responsable_nombre,
                    int(vehiculo_id),
                    responsable_id_db,
                    int(no_pasajeros_int),
                    " -> ".join(destinos),
                    motivo_salida.strip(),
                ))
                break
            except sqlite3.IntegrityError as exc:
                if "movimientos.folio" in str(exc) and intento < max_intentos - 1:
                    continue
                conn.close()
                return False, {"mensaje": "No se pudo generar un folio unico. Intente de nuevo."}
            except sqlite3.OperationalError as exc:
                if "database is locked" in str(exc).lower() and intento < max_intentos - 1:
                    time.sleep(0.2)
                    continue
                conn.close()
                return False, {"mensaje": "No se pudo registrar el movimiento. Intente de nuevo."}

        movimiento_id = cur.lastrowid
        if pasajeros_ids:
            cur.executemany("""
                INSERT INTO movimientos_auditores (movimiento_id, auditor_id)
                VALUES (?, ?)
            """, [(movimiento_id, pasajero_id) for pasajero_id in pasajeros_ids])
        if destinos:
            cur.executemany("""
                INSERT INTO movimientos_destinos (movimiento_id, ente_clave, orden)
                VALUES (?, ?, ?)
            """, [
                (movimiento_id, clave, orden)
                for orden, clave in enumerate(destinos, start=1)
            ])
        self._registrar_evento(cur, movimiento_id, usuario_id, "SOLICITADO", "")
        conn.commit()
        conn.close()
        return True, {"folio": folio, "movimiento_id": movimiento_id}

    def crear_movimiento_emergencia(
        self,
        monitor_id: int,
        vehiculo_id: int,
        responsable_auditor_id: int,
        no_pasajeros: int,
        pasajeros_ids: List[int],
        ruta_destinos: List[str],
        motivo_salida: str,
        observaciones: Optional[str] = None,
        fecha_solicitud: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        fecha_txt = _parse_date(fecha_solicitud) or _hoy_iso()
        if fecha_txt != _hoy_iso():
            return False, {"mensaje": "Los movimientos de emergencia solo se pueden registrar para la fecha actual."}

        nota_emergencia = (observaciones or "").strip()
        if nota_emergencia:
            nota_emergencia = f"Emergencia: {nota_emergencia}"
        else:
            nota_emergencia = "Emergencia registrada por monitor."

        ok, data = self.crear_movimiento(
            monitor_id,
            ruta_destinos[0] if ruta_destinos else "",
            1,
            "",
            None,
            nota_emergencia,
            None,
            vehiculo_id,
            responsable_auditor_id,
            "auditor",
            no_pasajeros,
            pasajeros_ids,
            ruta_destinos,
            motivo_salida,
            fecha_solicitud=fecha_txt,
        )
        if not ok:
            return ok, data

        movimiento_id = data["movimiento_id"]
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE movimientos
            SET observaciones=?,
                es_emergencia=1
            WHERE id=?
        """, (
            nota_emergencia,
            movimiento_id,
        ))
        self._registrar_evento(cur, movimiento_id, monitor_id, "EMERGENCIA", "Alta directa por monitor.")
        conn.commit()
        conn.close()

        ok_entrega, mensaje_entrega = self.marcar_entregado(movimiento_id, monitor_id)
        if not ok_entrega:
            return False, {"mensaje": mensaje_entrega}

        data["es_emergencia"] = 1
        return True, data

    def listar_movimientos(self, usuario_id: Optional[int] = None) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        q = """
            SELECT m.id, m.folio, m.ente_clave, m.fecha_solicitud,
                   m.created_at,
                   m.fecha_entrega, m.fecha_devolucion, m.cantidad,
                   m.receptor_nombre, m.firma_recepcion,
                   m.devuelto, m.observaciones, m.resguardante_nombre,
                   COALESCE(m.placa_unidad, v.placa) AS placa_unidad,
                   COALESCE(m.marca, v.marca) AS marca,
                   COALESCE(m.modelo, v.modelo) AS modelo,
                   COALESCE(m.responsable_vehiculo, uresp.nombre) AS responsable_vehiculo,
                   COALESCE((
                       SELECT group_concat(u3.nombre, ', ')
                       FROM usuarios_vehiculos uv3
                       JOIN usuarios u3 ON u3.id = uv3.usuario_id
                       WHERE uv3.vehiculo_id = v.id
                   ), "") AS propietarios_nombres,
                   m.vehiculo_id, m.responsable_id,
                   m.no_pasajeros,
                   COALESCE((
                       SELECT group_concat(nombre, ', ')
                       FROM (
                           SELECT a2.nombre AS nombre
                           FROM movimientos_auditores ma
                           JOIN auditores a2 ON a2.id = ma.auditor_id
                           WHERE ma.movimiento_id = m.id
                             AND a2.nombre != COALESCE(m.responsable_vehiculo, uresp.nombre)
                           ORDER BY a2.nombre
                       )
                   ), "") AS pasajeros_nombres,
                   COALESCE((
                       SELECT group_concat(destino, ' -> ')
                       FROM (
                           SELECT CASE
                               WHEN e2.clave = 'CCLET' THEN e2.clave
                               ELSE COALESCE(e2.nombre, md.ente_clave)
                           END AS destino
                           FROM movimientos_destinos md
                           LEFT JOIN entes e2 ON e2.clave = md.ente_clave
                           WHERE md.movimiento_id = m.id
                           ORDER BY md.orden
                       )
                   ), m.ruta_destino) AS ruta_destino,
                   m.motivo_salida,
                   COALESCE(m.es_emergencia, 0) AS es_emergencia,
                   EXISTS(
                       SELECT 1
                       FROM movimientos_eventos me
                       WHERE me.movimiento_id = m.id
                         AND me.evento = 'RECHAZADO'
                   ) AS rechazado,
                   e.nombre AS ente_nombre,
                   u.nombre AS usuario_nombre
            FROM movimientos m
            JOIN usuarios u ON u.id = m.usuario_id
            LEFT JOIN entes e ON e.clave = m.ente_clave
            LEFT JOIN vehiculos v ON v.id = m.vehiculo_id
            LEFT JOIN usuarios uresp ON uresp.id = m.responsable_id
        """
        params = ()
        if usuario_id:
            q += " WHERE m.usuario_id=?"
            params = (usuario_id,)
        q += " ORDER BY m.created_at DESC"
        cur.execute(q, params)
        data = [dict(r) for r in cur.fetchall()]
        for mov in data:
            self._aplicar_resguardante_asignado(mov)
            mov["tipo"] = "movimiento"
            mov["hora_solicitud_mx"] = _hora_mexico_desde_created_at(mov.get("created_at"))

        prestamos_q = """
            SELECT p.id, p.solicitante_id, p.propietario_id, p.vehiculo_id,
                   p.fecha_solicitud, p.estado, p.notas, p.fechas_solicitadas,
                   p.responsable_id, p.responsable_nombre, p.no_pasajeros,
                   p.pasajeros_ids, p.ruta_destino, p.motivo_salida,
                   v.placa AS placa_unidad, v.marca, v.modelo,
                   us.nombre AS solicitante_nombre,
                   up.nombre AS propietario_nombre
            FROM prestamos_vehiculos p
            JOIN usuarios us ON us.id = p.solicitante_id
            JOIN usuarios up ON up.id = p.propietario_id
            LEFT JOIN vehiculos v ON v.id = p.vehiculo_id
        """
        prestamos_params: Tuple[Union[int, str], ...] = ()
        if usuario_id:
            prestamos_q += " WHERE p.solicitante_id=?"
            prestamos_params = (usuario_id,)
        prestamos_q += " ORDER BY p.id DESC"
        cur.execute(prestamos_q, prestamos_params)
        prestamos_rows = [dict(r) for r in cur.fetchall()]

        pasajeros_ids: Set[int] = set()
        for row in prestamos_rows:
            pasajeros_txt = row.get("pasajeros_ids") or ""
            for raw_id in pasajeros_txt.split(","):
                raw_id = raw_id.strip()
                if raw_id.isdigit():
                    pasajeros_ids.add(int(raw_id))

        pasajeros_nombres_por_id: Dict[int, str] = {}
        if pasajeros_ids:
            placeholders = ",".join("?" for _ in pasajeros_ids)
            cur.execute(
                f"SELECT id, nombre FROM auditores WHERE id IN ({placeholders})",
                tuple(sorted(pasajeros_ids)),
            )
            pasajeros_nombres_por_id = {
                int(row["id"]): row["nombre"]
                for row in cur.fetchall()
                if row["id"]
            }

        for row in prestamos_rows:
            fechas_txt = row["fechas_solicitadas"] or ""
            fecha_prestamo = ""
            for item in fechas_txt.split(","):
                item = item.strip()
                if item:
                    fecha_prestamo = item
                    break
            if not fecha_prestamo:
                fecha_prestamo = row["fecha_solicitud"]

            nombres_pasajeros: List[str] = []
            pasajeros_txt = row.get("pasajeros_ids") or ""
            for raw_id in pasajeros_txt.split(","):
                raw_id = raw_id.strip()
                if not raw_id.isdigit():
                    continue
                nombre = pasajeros_nombres_por_id.get(int(raw_id))
                if nombre:
                    nombres_pasajeros.append(nombre)

            estado = (row["estado"] or "").upper()
            data.append({
                "id": row["id"],
                "folio": f"PRE-{row['id']}",
                "ente_clave": None,
                "fecha_solicitud": fecha_prestamo,
                "created_at": None,
                "fecha_entrega": fecha_prestamo if estado == "VALIDADO" else None,
                "fecha_devolucion": None,
                "cantidad": 1,
                "receptor_nombre": None,
                "firma_recepcion": None,
                "devuelto": 0,
                "observaciones": row["notas"],
                "resguardante_nombre": row["propietario_nombre"],
                "resguardante_id": row["propietario_id"],
                "placa_unidad": row["placa_unidad"],
                "marca": row["marca"],
                "modelo": row["modelo"],
                "responsable_vehiculo": row["responsable_nombre"] or row["propietario_nombre"],
                "propietarios_nombres": row["propietario_nombre"],
                "vehiculo_id": row["vehiculo_id"],
                "responsable_id": row["responsable_id"],
                "no_pasajeros": row["no_pasajeros"],
                "pasajeros_nombres": ", ".join(nombres_pasajeros),
                "ruta_destino": self._formatear_ruta_destino_con_entes_cursor(cur, row["ruta_destino"]),
                "motivo_salida": row["motivo_salida"],
                "es_emergencia": 0,
                "rechazado": 1 if estado == "RECHAZADO" else 0,
                "ente_nombre": None,
                "usuario_nombre": row["solicitante_nombre"],
                "tipo": "prestamo",
                "estado": estado,
                "hora_solicitud_mx": "-",
            })

        conn.close()
        pendientes_por_vehiculo = {}
        for mov in data:
            if (
                mov.get("vehiculo_id")
                and mov.get("fecha_solicitud")
                and not mov.get("fecha_entrega")
                and not mov.get("devuelto")
                and not mov.get("rechazado")
            ):
                clave = (mov["vehiculo_id"], mov["fecha_solicitud"])
                pendientes_por_vehiculo[clave] = pendientes_por_vehiculo.get(clave, 0) + 1
        for mov in data:
            clave = None
            if mov.get("vehiculo_id") and mov.get("fecha_solicitud"):
                clave = (mov["vehiculo_id"], mov["fecha_solicitud"])
            mov["conflicto"] = bool(clave and pendientes_por_vehiculo.get(clave, 0) > 1)
        return data

    def listar_movimientos_por_usuarios(self, usuarios: List[str]) -> List[Dict]:
        usuarios_norm = [u.strip().lower() for u in usuarios if u and u.strip()]
        if not usuarios_norm:
            return []
        placeholders = ",".join(["?"] * len(usuarios_norm))

        conn = self._connect()
        cur = conn.cursor()
        q = f"""
            SELECT m.id, m.folio, m.ente_clave, m.fecha_solicitud,
                   m.created_at,
                   m.fecha_entrega, m.fecha_devolucion, m.cantidad,
                   m.receptor_nombre, m.firma_recepcion,
                   m.devuelto, m.observaciones, m.resguardante_nombre,
                   COALESCE(m.placa_unidad, v.placa) AS placa_unidad,
                   COALESCE(m.marca, v.marca) AS marca,
                   COALESCE(m.modelo, v.modelo) AS modelo,
                   COALESCE(m.responsable_vehiculo, uresp.nombre) AS responsable_vehiculo,
                   COALESCE((
                       SELECT group_concat(u3.nombre, ', ')
                       FROM usuarios_vehiculos uv3
                       JOIN usuarios u3 ON u3.id = uv3.usuario_id
                       WHERE uv3.vehiculo_id = v.id
                   ), "") AS propietarios_nombres,
                   m.vehiculo_id, m.responsable_id,
                   m.no_pasajeros,
                   COALESCE((
                       SELECT group_concat(nombre, ', ')
                       FROM (
                           SELECT a2.nombre AS nombre
                           FROM movimientos_auditores ma
                           JOIN auditores a2 ON a2.id = ma.auditor_id
                           WHERE ma.movimiento_id = m.id
                             AND a2.nombre != COALESCE(m.responsable_vehiculo, uresp.nombre)
                           ORDER BY a2.nombre
                       )
                   ), "") AS pasajeros_nombres,
                   COALESCE((
                       SELECT group_concat(destino, ' -> ')
                       FROM (
                           SELECT CASE
                               WHEN e2.clave = 'CCLET' THEN e2.clave
                               ELSE COALESCE(e2.nombre, md.ente_clave)
                           END AS destino
                           FROM movimientos_destinos md
                           LEFT JOIN entes e2 ON e2.clave = md.ente_clave
                           WHERE md.movimiento_id = m.id
                           ORDER BY md.orden
                       )
                   ), m.ruta_destino) AS ruta_destino,
                   m.motivo_salida,
                   COALESCE(m.es_emergencia, 0) AS es_emergencia,
                   EXISTS(
                       SELECT 1
                       FROM movimientos_eventos me
                       WHERE me.movimiento_id = m.id
                         AND me.evento = 'RECHAZADO'
                   ) AS rechazado,
                   e.nombre AS ente_nombre,
                   u.nombre AS usuario_nombre
            FROM movimientos m
            JOIN usuarios u ON u.id = m.usuario_id
            LEFT JOIN entes e ON e.clave = m.ente_clave
            LEFT JOIN vehiculos v ON v.id = m.vehiculo_id
            LEFT JOIN usuarios uresp ON uresp.id = m.responsable_id
            WHERE LOWER(u.usuario) IN ({placeholders})
            ORDER BY m.created_at DESC
        """
        cur.execute(q, usuarios_norm)
        data = [dict(r) for r in cur.fetchall()]
        for mov in data:
            self._aplicar_resguardante_asignado(mov)
            mov["tipo"] = "movimiento"
            mov["hora_solicitud_mx"] = _hora_mexico_desde_created_at(mov.get("created_at"))

        cur.execute(f"""
            SELECT p.id, p.solicitante_id, p.propietario_id, p.vehiculo_id,
                   p.fecha_solicitud, p.estado, p.notas, p.fechas_solicitadas,
                   p.responsable_id, p.responsable_nombre, p.no_pasajeros,
                   p.ruta_destino, p.motivo_salida,
                   v.placa AS placa_unidad, v.marca, v.modelo,
                   us.nombre AS solicitante_nombre,
                   up.nombre AS propietario_nombre
            FROM prestamos_vehiculos p
            JOIN usuarios us ON us.id = p.solicitante_id
            JOIN usuarios up ON up.id = p.propietario_id
            LEFT JOIN vehiculos v ON v.id = p.vehiculo_id
            WHERE LOWER(us.usuario) IN ({placeholders})
            ORDER BY p.id DESC
        """, usuarios_norm)
        for row in cur.fetchall():
            fechas_txt = row["fechas_solicitadas"] or ""
            fecha_prestamo = ""
            for item in fechas_txt.split(","):
                item = item.strip()
                if item:
                    fecha_prestamo = item
                    break
            if not fecha_prestamo:
                fecha_prestamo = row["fecha_solicitud"]
            estado = (row["estado"] or "").upper()
            data.append({
                "id": row["id"],
                "folio": f"PRE-{row['id']}",
                "ente_clave": None,
                "fecha_solicitud": fecha_prestamo,
                "created_at": None,
                "fecha_entrega": fecha_prestamo if estado == "VALIDADO" else None,
                "fecha_devolucion": None,
                "cantidad": 1,
                "receptor_nombre": None,
                "firma_recepcion": None,
                "devuelto": 0,
                "observaciones": row["notas"],
                "resguardante_nombre": row["propietario_nombre"],
                "resguardante_id": row["propietario_id"],
                "placa_unidad": row["placa_unidad"],
                "marca": row["marca"],
                "modelo": row["modelo"],
                "responsable_vehiculo": row["responsable_nombre"],
                "propietarios_nombres": row["propietario_nombre"],
                "vehiculo_id": row["vehiculo_id"],
                "responsable_id": row["responsable_id"],
                "no_pasajeros": row["no_pasajeros"],
                "pasajeros_nombres": "",
                "ruta_destino": self._formatear_ruta_destino_con_entes_cursor(cur, row["ruta_destino"]),
                "motivo_salida": row["motivo_salida"],
                "es_emergencia": 0,
                "rechazado": 1 if estado == "RECHAZADO" else 0,
                "ente_nombre": None,
                "usuario_nombre": row["solicitante_nombre"],
                "tipo": "prestamo",
                "estado": estado,
                "hora_solicitud_mx": "-",
            })

        conn.close()
        pendientes_por_vehiculo = {}
        for mov in data:
            if (
                mov.get("vehiculo_id")
                and mov.get("fecha_solicitud")
                and not mov.get("fecha_entrega")
                and not mov.get("devuelto")
                and not mov.get("rechazado")
            ):
                clave = (mov["vehiculo_id"], mov["fecha_solicitud"])
                pendientes_por_vehiculo[clave] = pendientes_por_vehiculo.get(clave, 0) + 1
        for mov in data:
            clave = None
            if mov.get("vehiculo_id") and mov.get("fecha_solicitud"):
                clave = (mov["vehiculo_id"], mov["fecha_solicitud"])
            mov["conflicto"] = bool(clave and pendientes_por_vehiculo.get(clave, 0) > 1)
        return data

    def listar_agenda_semanal(self) -> Dict:
        hoy = date.today()
        lunes = hoy - timedelta(days=hoy.weekday())
        fechas = [lunes + timedelta(days=offset) for offset in range(5)]
        fechas_iso = {fecha.isoformat() for fecha in fechas}
        labels = ["Lun", "Mar", "Mie", "Jue", "Vie"]
        dias = [
            {
                "fecha": fecha.isoformat(),
                "label": f"{labels[idx]} {fecha.day:02d}/{fecha.month:02d}",
            }
            for idx, fecha in enumerate(fechas)
        ]

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, vehiculo_id, fechas_solicitadas
            FROM prestamos_vehiculos
            WHERE estado='PENDIENTE'
        """)
        reservas_por_vehiculo = {}
        for row in cur.fetchall():
            vehiculo_id = row["vehiculo_id"]
            fechas_txt = row["fechas_solicitadas"] or ""
            fechas_reserva = {
                fecha.strip()
                for fecha in fechas_txt.split(",")
                if fecha.strip() in fechas_iso
            }
            if not fechas_reserva:
                continue
            reservas_por_vehiculo.setdefault(vehiculo_id, set()).update(fechas_reserva)

        cur.execute("""
            SELECT v.id, v.placa, v.marca, v.modelo, v.categoria,
                   COALESCE(GROUP_CONCAT(u.nombre, ', '), '') AS propietarios
            FROM vehiculos v
            LEFT JOIN usuarios_vehiculos uv ON uv.vehiculo_id = v.id
            LEFT JOIN usuarios u ON u.id = uv.usuario_id
            WHERE v.activo=1
            GROUP BY v.id
            ORDER BY v.placa
        """)
        vehiculos = []
        for row in cur.fetchall():
            item = dict(row)
            item["reservas"] = reservas_por_vehiculo.get(item["id"], set())
            vehiculos.append(item)
        conn.close()

        return {
            "inicio": lunes.isoformat(),
            "fin": fechas[-1].isoformat(),
            "fechas": dias,
            "vehiculos": vehiculos,
        }

    def listar_movimientos_entregados(self, usuario_id: int, fecha_iso: str) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT m.id, m.folio, m.ente_clave, m.fecha_solicitud,
                   m.fecha_entrega, m.fecha_devolucion, m.cantidad,
                   m.receptor_nombre, m.firma_recepcion,
                   m.devuelto, m.observaciones, m.resguardante_nombre,
                   COALESCE(m.placa_unidad, v.placa) AS placa_unidad,
                   COALESCE(m.marca, v.marca) AS marca,
                   COALESCE(m.modelo, v.modelo) AS modelo,
                   COALESCE(m.responsable_vehiculo, uresp.nombre) AS responsable_vehiculo,
                   COALESCE((
                       SELECT group_concat(nombre, ', ')
                       FROM (
                           SELECT u3.nombre AS nombre
                           FROM usuarios_vehiculos uv3
                           JOIN usuarios u3 ON u3.id = uv3.usuario_id
                           WHERE uv3.vehiculo_id = v.id
                           ORDER BY u3.nombre
                       )
                   ), "") AS propietarios_nombres,
                   m.vehiculo_id, m.responsable_id,
                   m.no_pasajeros,
                   COALESCE((
                       SELECT group_concat(nombre, ', ')
                       FROM (
                           SELECT a2.nombre AS nombre
                           FROM movimientos_auditores ma
                           JOIN auditores a2 ON a2.id = ma.auditor_id
                           WHERE ma.movimiento_id = m.id
                             AND a2.nombre != COALESCE(m.responsable_vehiculo, uresp.nombre)
                           ORDER BY a2.nombre
                       )
                   ), "") AS pasajeros_nombres,
                   COALESCE((
                       SELECT group_concat(destino, ' -> ')
                       FROM (
                           SELECT CASE
                               WHEN e2.clave = 'CCLET' THEN e2.clave
                               ELSE COALESCE(e2.nombre, md.ente_clave)
                           END AS destino
                           FROM movimientos_destinos md
                           LEFT JOIN entes e2 ON e2.clave = md.ente_clave
                           WHERE md.movimiento_id = m.id
                           ORDER BY md.orden
                       )
                   ), m.ruta_destino) AS ruta_destino,
                   m.motivo_salida,
                   COALESCE(m.es_emergencia, 0) AS es_emergencia,
                   e.nombre AS ente_nombre,
                   u.nombre AS usuario_nombre
            FROM movimientos m
            JOIN usuarios u ON u.id = m.usuario_id
            LEFT JOIN entes e ON e.clave = m.ente_clave
            LEFT JOIN vehiculos v ON v.id = m.vehiculo_id
            LEFT JOIN usuarios uresp ON uresp.id = m.responsable_id
            WHERE m.fecha_solicitud = ?
              AND m.fecha_entrega IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos_eventos me
                  WHERE me.movimiento_id = m.id
                    AND me.evento = 'RECHAZADO'
              )
            ORDER BY m.created_at DESC
        """, (fecha_iso,))
        data = [dict(r) for r in cur.fetchall()]
        for mov in data:
            self._aplicar_resguardante_asignado(mov)
        cur.execute("""
            SELECT p.id, p.solicitante_id, p.propietario_id, p.vehiculo_id,
                   p.fecha_solicitud, p.estado, p.notas, p.fechas_solicitadas,
                   p.responsable_id, p.responsable_nombre, p.no_pasajeros,
                   p.pasajeros_ids, p.ruta_destino, p.motivo_salida,
                   v.placa AS placa_unidad, v.marca, v.modelo,
                   us.nombre AS solicitante_nombre,
                   up.nombre AS propietario_nombre
            FROM prestamos_vehiculos p
            JOIN usuarios us ON us.id = p.solicitante_id
            JOIN usuarios up ON up.id = p.propietario_id
            LEFT JOIN vehiculos v ON v.id = p.vehiculo_id
            WHERE p.estado = 'VALIDADO'
              AND (
                (',' || COALESCE(p.fechas_solicitadas, '') || ',') LIKE '%,' || ? || ',%'
                OR p.fecha_solicitud = ?
              )
            ORDER BY p.id DESC
        """, (fecha_iso, fecha_iso))
        prestamos_rows = [dict(r) for r in cur.fetchall()]
        pasajeros_ids: Set[int] = set()
        for row in prestamos_rows:
            pasajeros_txt = row.get("pasajeros_ids") or ""
            for raw_id in pasajeros_txt.split(","):
                raw_id = raw_id.strip()
                if raw_id.isdigit():
                    pasajeros_ids.add(int(raw_id))
        pasajeros_nombres_por_id: Dict[int, str] = {}
        if pasajeros_ids:
            placeholders = ",".join("?" for _ in pasajeros_ids)
            cur.execute(
                f"SELECT id, nombre FROM auditores WHERE id IN ({placeholders})",
                tuple(sorted(pasajeros_ids)),
            )
            pasajeros_nombres_por_id = {
                int(row["id"]): row["nombre"]
                for row in cur.fetchall()
                if row["id"]
            }
        for row in prestamos_rows:
            nombres_pasajeros: List[str] = []
            pasajeros_txt = row.get("pasajeros_ids") or ""
            for raw_id in pasajeros_txt.split(","):
                raw_id = raw_id.strip()
                if not raw_id.isdigit():
                    continue
                nombre = pasajeros_nombres_por_id.get(int(raw_id))
                if nombre:
                    nombres_pasajeros.append(nombre)
            data.append({
                "id": row["id"],
                "folio": f"PRE-{row['id']}",
                "ente_clave": None,
                "fecha_solicitud": fecha_iso,
                "fecha_entrega": fecha_iso,
                "fecha_devolucion": None,
                "cantidad": 1,
                "receptor_nombre": None,
                "firma_recepcion": None,
                "devuelto": 0,
                "observaciones": row["notas"],
                "resguardante_nombre": row["propietario_nombre"],
                "placa_unidad": row["placa_unidad"],
                "marca": row["marca"],
                "modelo": row["modelo"],
                "responsable_vehiculo": row["responsable_nombre"] or row["propietario_nombre"],
                "vehiculo_id": row["vehiculo_id"],
                "responsable_id": row["responsable_id"],
                "no_pasajeros": row["no_pasajeros"],
                "pasajeros_nombres": ", ".join(nombres_pasajeros),
                "ruta_destino": self._formatear_ruta_destino_con_entes_cursor(cur, row["ruta_destino"]),
                "motivo_salida": row["motivo_salida"],
                "es_emergencia": 0,
                "ente_nombre": None,
                "usuario_nombre": row["solicitante_nombre"],
                "tipo": "prestamo",
            })
        conn.close()
        return data

    def obtener_movimiento(self, movimiento_id: int) -> Optional[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT m.id, m.folio, m.ente_clave, m.fecha_solicitud,
                   m.fecha_entrega, m.fecha_devolucion, m.cantidad,
                   m.receptor_nombre, m.firma_recepcion,
                   m.devuelto, m.observaciones, m.resguardante_nombre,
                   COALESCE(m.placa_unidad, v.placa) AS placa_unidad,
                   COALESCE(m.marca, v.marca) AS marca,
                   COALESCE(m.modelo, v.modelo) AS modelo,
                   COALESCE(m.responsable_vehiculo, uresp.nombre) AS responsable_vehiculo,
                   COALESCE((
                       SELECT group_concat(nombre, ', ')
                       FROM (
                           SELECT u3.nombre AS nombre
                           FROM usuarios_vehiculos uv3
                           JOIN usuarios u3 ON u3.id = uv3.usuario_id
                           WHERE uv3.vehiculo_id = v.id
                           ORDER BY u3.nombre
                       )
                   ), "") AS propietarios_nombres,
                   m.vehiculo_id, m.responsable_id,
                   m.no_pasajeros,
                   COALESCE((
                       SELECT group_concat(nombre, ', ')
                       FROM (
                           SELECT a2.nombre AS nombre
                           FROM movimientos_auditores ma
                           JOIN auditores a2 ON a2.id = ma.auditor_id
                           WHERE ma.movimiento_id = m.id
                             AND a2.nombre != COALESCE(m.responsable_vehiculo, uresp.nombre)
                           ORDER BY a2.nombre
                       )
                   ), "") AS pasajeros_nombres,
                   COALESCE((
                       SELECT group_concat(destino, ' -> ')
                       FROM (
                           SELECT CASE
                               WHEN e2.clave = 'CCLET' THEN e2.clave
                               ELSE COALESCE(e2.nombre, md.ente_clave)
                           END AS destino
                           FROM movimientos_destinos md
                           LEFT JOIN entes e2 ON e2.clave = md.ente_clave
                           WHERE md.movimiento_id = m.id
                           ORDER BY md.orden
                       )
                   ), m.ruta_destino) AS ruta_destino,
                   m.motivo_salida,
                   COALESCE(m.es_emergencia, 0) AS es_emergencia,
                   u.nombre AS usuario_nombre,
                   e.nombre AS ente_nombre
            FROM movimientos m
            JOIN usuarios u ON u.id = m.usuario_id
            LEFT JOIN entes e ON e.clave = m.ente_clave
            LEFT JOIN vehiculos v ON v.id = m.vehiculo_id
            LEFT JOIN usuarios uresp ON uresp.id = m.responsable_id
            WHERE m.id=?
            LIMIT 1
        """, (movimiento_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return self._aplicar_resguardante_asignado(dict(row))

    def obtener_prestamo(self, prestamo_id: int) -> Optional[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT p.id, p.solicitante_id, p.propietario_id, p.vehiculo_id,
                   p.fecha_solicitud, p.estado, p.notas, p.fechas_solicitadas,
                   p.responsable_id, p.responsable_nombre, p.no_pasajeros,
                   p.pasajeros_ids, p.ruta_destino, p.motivo_salida,
                   v.placa AS placa_unidad, v.marca, v.modelo,
                   us.nombre AS solicitante_nombre,
                   up.nombre AS propietario_nombre
            FROM prestamos_vehiculos p
            JOIN usuarios us ON us.id = p.solicitante_id
            JOIN usuarios up ON up.id = p.propietario_id
            LEFT JOIN vehiculos v ON v.id = p.vehiculo_id
            WHERE p.id = ?
            LIMIT 1
        """, (prestamo_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return None

        fecha_prestamo = ""
        for item in (row["fechas_solicitadas"] or "").split(","):
            item = item.strip()
            if item:
                fecha_prestamo = item
                break
        if not fecha_prestamo:
            fecha_prestamo = row["fecha_solicitud"]

        pasajeros_ids = []
        for raw_id in (row["pasajeros_ids"] or "").split(","):
            raw_id = raw_id.strip()
            if raw_id.isdigit():
                pasajeros_ids.append(int(raw_id))

        pasajeros_nombres: List[str] = []
        if pasajeros_ids:
            placeholders = ",".join("?" for _ in pasajeros_ids)
            cur.execute(
                f"""
                    SELECT nombre
                    FROM auditores
                    WHERE id IN ({placeholders})
                    ORDER BY nombre
                """,
                tuple(pasajeros_ids),
            )
            pasajeros_nombres = [row_nombre["nombre"] for row_nombre in cur.fetchall() if row_nombre["nombre"]]

        data = {
            "id": row["id"],
            "folio": f"PRE-{row['id']}",
            "ente_clave": None,
            "fecha_solicitud": fecha_prestamo,
            "fecha_entrega": fecha_prestamo if (row["estado"] or "").upper() == "VALIDADO" else None,
            "fecha_devolucion": None,
            "cantidad": 1,
            "receptor_nombre": None,
            "firma_recepcion": None,
            "devuelto": 0,
            "observaciones": row["notas"],
            "resguardante_nombre": row["propietario_nombre"],
            "resguardante_id": row["propietario_id"],
            "placa_unidad": row["placa_unidad"],
            "marca": row["marca"],
            "modelo": row["modelo"],
            "responsable_vehiculo": row["responsable_nombre"] or row["propietario_nombre"],
            "vehiculo_id": row["vehiculo_id"],
            "responsable_id": row["responsable_id"],
            "no_pasajeros": row["no_pasajeros"],
            "pasajeros_nombres": ", ".join(pasajeros_nombres),
            "ruta_destino": self._formatear_ruta_destino_con_entes_cursor(cur, row["ruta_destino"]),
            "motivo_salida": row["motivo_salida"],
            "es_emergencia": 0,
            "usuario_nombre": row["solicitante_nombre"],
            "propietario_nombre": row["propietario_nombre"],
            "tipo": "prestamo",
            "estado": (row["estado"] or "").upper(),
        }
        conn.close()
        return data

    def obtener_solicitud_edicion(self, tipo: str, solicitud_id: int) -> Optional[Dict]:
        if tipo == "movimiento":
            solicitud = self.obtener_movimiento(solicitud_id)
        elif tipo == "prestamo":
            solicitud = self.obtener_prestamo(solicitud_id)
        else:
            return None
        if not solicitud:
            return None

        conn = self._connect()
        cur = conn.cursor()
        if tipo == "movimiento":
            cur.execute("""
                SELECT auditor_id
                FROM movimientos_auditores
                WHERE movimiento_id=?
                ORDER BY auditor_id
            """, (solicitud_id,))
            pasajeros_ids = [int(row["auditor_id"]) for row in cur.fetchall()]
            cur.execute("""
                SELECT ente_clave
                FROM movimientos_destinos
                WHERE movimiento_id=?
                ORDER BY orden
            """, (solicitud_id,))
            ruta_destinos = [row["ente_clave"] for row in cur.fetchall()]
            ruta_original = solicitud.get("ruta_destino")
        else:
            cur.execute("""
                SELECT pasajeros_ids, ruta_destino
                FROM prestamos_vehiculos
                WHERE id=?
            """, (solicitud_id,))
            row = cur.fetchone()
            pasajeros_ids = [
                int(raw_id)
                for raw_id in (row["pasajeros_ids"] or "").split(",")
                if raw_id.strip().isdigit()
            ]
            ruta_destinos = []
            ruta_original = row["ruta_destino"]

        if not ruta_destinos:
            ruta_destinos, _, _ = self._resolver_destinos_cursor(
                cur,
                _split_ruta_destino(ruta_original),
            )
        cur.execute("""
            SELECT id
            FROM auditores
            WHERE activo=1
              AND LOWER(TRIM(nombre)) = LOWER(TRIM(?))
            LIMIT 1
        """, (solicitud.get("responsable_vehiculo"),))
        responsable = cur.fetchone()
        conn.close()
        solicitud.update({
            "tipo": tipo,
            "responsable_auditor_id": responsable["id"] if responsable else None,
            "pasajeros_ids": pasajeros_ids,
            "ruta_destinos": ruta_destinos,
        })
        return solicitud

    def actualizar_solicitud_reporte(
        self,
        tipo: str,
        solicitud_id: int,
        vehiculo_id: int,
        fecha_solicitud: str,
        responsable_auditor_id: int,
        pasajeros_ids: List[int],
        ruta_destinos: List[str],
        motivo_salida: str,
        usuario_id: int,
    ) -> Tuple[bool, str]:
        if tipo not in {"movimiento", "prestamo"}:
            return False, "Tipo de solicitud no valido."
        fecha_txt = _parse_date(fecha_solicitud)
        if not fecha_txt:
            return False, "La fecha de salida no es valida."
        if motivo_salida not in MOTIVOS_SALIDA_VALIDOS:
            return False, "El motivo de salida no es valido."
        if len(pasajeros_ids) > 4:
            return False, "Solo se permiten hasta 4 acompanantes."
        if len(pasajeros_ids) != len(set(pasajeros_ids)):
            return False, "Hay acompanantes duplicados."
        if responsable_auditor_id in pasajeros_ids:
            return False, "El responsable no puede seleccionarse tambien como acompanante."

        conn = self._connect()
        cur = conn.cursor()
        if tipo == "movimiento":
            cur.execute("""
                SELECT id, usuario_id AS solicitante_id, NULL AS solicitante_usuario
                FROM movimientos m
                WHERE id=?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM movimientos_eventos me
                      WHERE me.movimiento_id=m.id AND me.evento='RECHAZADO'
                  )
            """, (solicitud_id,))
        else:
            cur.execute("""
                SELECT p.id, p.solicitante_id, u.usuario AS solicitante_usuario
                FROM prestamos_vehiculos p
                JOIN usuarios u ON u.id=p.solicitante_id
                WHERE p.id=? AND p.estado!='RECHAZADO'
            """, (solicitud_id,))
        solicitud = cur.fetchone()
        if not solicitud:
            conn.close()
            return False, "La solicitud no existe o fue rechazada."

        cur.execute("""
            SELECT id, placa, marca, modelo
            FROM vehiculos
            WHERE id=? AND activo=1
        """, (vehiculo_id,))
        vehiculo = cur.fetchone()
        if not vehiculo:
            conn.close()
            return False, "Vehiculo no encontrado."
        resguardante = self._obtener_resguardantes_vehiculo_cursor(cur, vehiculo_id)
        if not resguardante or resguardante["id"] is None:
            conn.close()
            return False, "La unidad debe tener un resguardante unico asignado."
        if tipo == "prestamo" and solicitud["solicitante_id"] == resguardante["id"]:
            conn.close()
            return False, "Un prestamo debe usar una unidad asignada a otro usuario."
        if (
            tipo == "prestamo"
            and vehiculo["placa"].strip().upper() in PLACAS_SOLO_PROPIETARIO
            and (solicitud["solicitante_usuario"] or "").strip().lower() != "angel"
        ):
            conn.close()
            return False, "El vehiculo solo esta disponible para Angel."

        movimiento_excluido = solicitud_id if tipo == "movimiento" else -1
        prestamo_excluido = solicitud_id if tipo == "prestamo" else -1
        cur.execute("""
            SELECT 1
            FROM movimientos m
            WHERE m.vehiculo_id=? AND m.fecha_solicitud=? AND m.id!=?
              AND NOT EXISTS (
                  SELECT 1 FROM movimientos_eventos me
                  WHERE me.movimiento_id=m.id AND me.evento='RECHAZADO'
              )
            LIMIT 1
        """, (vehiculo_id, fecha_txt, movimiento_excluido))
        vehiculo_ocupado = bool(cur.fetchone())
        if not vehiculo_ocupado:
            cur.execute("""
                SELECT 1
                FROM prestamos_vehiculos p
                WHERE p.vehiculo_id=? AND p.estado IN ('PENDIENTE', 'VALIDADO') AND p.id!=?
                  AND (
                      (
                          TRIM(COALESCE(p.fechas_solicitadas, '')) != ''
                          AND (',' || p.fechas_solicitadas || ',') LIKE '%,' || ? || ',%'
                      )
                      OR (
                          TRIM(COALESCE(p.fechas_solicitadas, '')) = ''
                          AND p.fecha_solicitud=?
                      )
                  )
                LIMIT 1
            """, (vehiculo_id, prestamo_excluido, fecha_txt, fecha_txt))
            vehiculo_ocupado = bool(cur.fetchone())
        if vehiculo_ocupado:
            conn.close()
            return False, "La unidad ya esta asignada para esa fecha."

        responsable = self._obtener_responsable(cur, "auditor", responsable_auditor_id)
        if not responsable:
            conn.close()
            return False, "Responsable no encontrado."
        responsable_nombre, responsable_id = responsable

        if pasajeros_ids:
            placeholders = ",".join("?" for _ in pasajeros_ids)
            cur.execute(
                f"SELECT id FROM auditores WHERE activo=1 AND id IN ({placeholders})",
                pasajeros_ids,
            )
            if len(cur.fetchall()) != len(pasajeros_ids):
                conn.close()
                return False, "Uno o mas acompanantes no existen."
        ocupados = self._obtener_auditores_ocupados_cursor(
            cur,
            fecha_txt,
            movimiento_excluido if movimiento_excluido > 0 else None,
            prestamo_excluido if prestamo_excluido > 0 else None,
        )
        if responsable_id in ocupados or any(pid in ocupados for pid in pasajeros_ids):
            conn.close()
            return False, "El responsable o uno de los acompanantes ya esta asignado para esa fecha."

        destinos, nombres_destinos, destinos_faltantes = self._resolver_destinos_cursor(
            cur,
            ruta_destinos,
        )
        if not destinos:
            conn.close()
            return False, "Falta seleccionar la ruta destino."
        if destinos_faltantes:
            conn.close()
            return False, "Uno o mas destinos no existen."
        ruta_legible = " -> ".join(
            "CCLET" if clave == "CCLET" else (nombres_destinos.get(clave) or clave)
            for clave in destinos
        )

        try:
            if tipo == "movimiento":
                cur.execute("""
                    UPDATE movimientos
                    SET ente_clave=?, fecha_solicitud=?, resguardante_nombre=?, resguardante_id=?,
                        placa_unidad=?, marca=?, modelo=?, responsable_vehiculo=?, vehiculo_id=?,
                        responsable_id=?, no_pasajeros=?, ruta_destino=?, motivo_salida=?
                    WHERE id=?
                """, (
                    destinos[0], fecha_txt, resguardante["nombre"], resguardante["id"],
                    vehiculo["placa"], vehiculo["marca"], vehiculo["modelo"],
                    responsable_nombre, vehiculo_id, responsable_id, len(pasajeros_ids),
                    " -> ".join(destinos), motivo_salida, solicitud_id,
                ))
                cur.execute("DELETE FROM movimientos_auditores WHERE movimiento_id=?", (solicitud_id,))
                cur.executemany(
                    "INSERT INTO movimientos_auditores (movimiento_id, auditor_id) VALUES (?, ?)",
                    [(solicitud_id, auditor_id) for auditor_id in pasajeros_ids],
                )
                cur.execute("DELETE FROM movimientos_destinos WHERE movimiento_id=?", (solicitud_id,))
                cur.executemany(
                    """
                        INSERT INTO movimientos_destinos (movimiento_id, ente_clave, orden)
                        VALUES (?, ?, ?)
                    """,
                    [
                        (solicitud_id, clave, orden)
                        for orden, clave in enumerate(destinos, start=1)
                    ],
                )
                self._registrar_evento(
                    cur,
                    solicitud_id,
                    usuario_id,
                    "EDITADO",
                    "Datos del reporte actualizados por monitor.",
                )
            else:
                cur.execute("""
                    UPDATE prestamos_vehiculos
                    SET propietario_id=?, vehiculo_id=?, fecha_solicitud=?, fechas_solicitadas=?,
                        responsable_id=?, responsable_nombre=?, no_pasajeros=?, pasajeros_ids=?,
                        ruta_destino=?, motivo_salida=?
                    WHERE id=?
                """, (
                    resguardante["id"], vehiculo_id, fecha_txt, fecha_txt,
                    responsable_id, responsable_nombre, len(pasajeros_ids),
                    ",".join(str(pid) for pid in pasajeros_ids) or None,
                    ruta_legible, motivo_salida, solicitud_id,
                ))
            conn.commit()
        except sqlite3.Error:
            conn.rollback()
            logger.exception("No se pudo actualizar la solicitud %s %s", tipo, solicitud_id)
            conn.close()
            return False, "No se pudo guardar la solicitud. Intente de nuevo."
        conn.close()
        return True, "Solicitud actualizada correctamente."

    def marcar_entregado(self, movimiento_id: int, usuario_id: int) -> Tuple[bool, str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, fecha_entrega, devuelto, vehiculo_id, fecha_solicitud
            FROM movimientos
            WHERE id=?
        """, (movimiento_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False, "Movimiento no encontrado."
        cur.execute("""
            SELECT 1
            FROM movimientos_eventos
            WHERE movimiento_id=?
              AND evento='RECHAZADO'
            LIMIT 1
        """, (movimiento_id,))
        if cur.fetchone():
            conn.close()
            return False, "El movimiento fue rechazado."
        if row["fecha_entrega"]:
            conn.close()
            return False, "El movimiento ya fue entregado."
        vehiculo_id = row["vehiculo_id"]
        fecha_solicitud = row["fecha_solicitud"]
        if vehiculo_id:
            cur.execute("""
                SELECT 1
                FROM movimientos m
                WHERE m.vehiculo_id=?
                  AND m.id != ?
                  AND m.fecha_solicitud=?
                  AND m.fecha_entrega IS NOT NULL
                  AND m.devuelto=0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM movimientos_eventos me
                      WHERE me.movimiento_id = m.id
                        AND me.evento = 'RECHAZADO'
                  )
                LIMIT 1
            """, (vehiculo_id, movimiento_id, fecha_solicitud))
            if cur.fetchone():
                conn.close()
                return False, "Ya existe una entrega validada para esta unidad en la misma fecha solicitada."
        cur.execute("""
            UPDATE movimientos
            SET fecha_entrega=?
            WHERE id=?
        """, (_hoy_iso(), movimiento_id))
        self._registrar_evento(cur, movimiento_id, usuario_id, "ENTREGADO", "")
        if vehiculo_id:
            cur.execute("""
                SELECT m.id
                FROM movimientos m
                WHERE m.vehiculo_id=?
                  AND m.id != ?
                  AND m.fecha_solicitud=?
                  AND m.fecha_entrega IS NULL
                  AND m.devuelto=0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM movimientos_eventos me
                      WHERE me.movimiento_id = m.id
                        AND me.evento = 'RECHAZADO'
                  )
            """, (vehiculo_id, movimiento_id, fecha_solicitud))
            for pendiente in cur.fetchall():
                self._registrar_evento(
                    cur,
                    pendiente["id"],
                    usuario_id,
                    "RECHAZADO",
                    "Auto rechazado: unidad ya validada.",
                )
        conn.commit()
        conn.close()
        return True, "Entrega registrada."

    def marcar_rechazado(self, movimiento_id: int, usuario_id: int) -> Tuple[bool, str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, fecha_entrega, devuelto
            FROM movimientos
            WHERE id=?
        """, (movimiento_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False, "Movimiento no encontrado."
        if row["fecha_entrega"] or row["devuelto"]:
            conn.close()
            return False, "No se puede rechazar un movimiento entregado."
        cur.execute("""
            SELECT 1
            FROM movimientos_eventos
            WHERE movimiento_id=?
              AND evento='RECHAZADO'
            LIMIT 1
        """, (movimiento_id,))
        if cur.fetchone():
            conn.close()
            return False, "El movimiento ya fue rechazado."
        self._registrar_evento(cur, movimiento_id, usuario_id, "RECHAZADO", "")
        conn.commit()
        conn.close()
        return True, "Movimiento rechazado."

    def marcar_prestamo_validado(self, prestamo_id: int, usuario_id: int) -> Tuple[bool, str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, vehiculo_id, fechas_solicitadas, estado
            FROM prestamos_vehiculos
            WHERE id=?
        """, (prestamo_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False, "Prestamo no encontrado."
        estado = (row["estado"] or "").upper()
        if estado == "RECHAZADO":
            conn.close()
            return False, "El prestamo ya fue rechazado."
        if estado == "VALIDADO":
            conn.close()
            return False, "El prestamo ya fue validado."
        fecha_prestamo = ""
        for item in (row["fechas_solicitadas"] or "").split(","):
            item = _parse_date(item.strip())
            if item:
                fecha_prestamo = item
                break
        if not fecha_prestamo:
            conn.close()
            return False, "Fecha de prestamo no valida."
        cur.execute("""
            SELECT 1
            FROM movimientos m
            WHERE m.vehiculo_id=?
              AND m.fecha_solicitud=?
              AND NOT EXISTS (
                  SELECT 1
                  FROM movimientos_eventos me
                  WHERE me.movimiento_id = m.id
                    AND me.evento = 'RECHAZADO'
              )
            LIMIT 1
        """, (row["vehiculo_id"], fecha_prestamo))
        if cur.fetchone():
            conn.close()
            return False, "El vehiculo ya esta asignado para esa fecha."
        cur.execute("""
            SELECT 1
            FROM prestamos_vehiculos p
            WHERE p.vehiculo_id=?
              AND p.estado='VALIDADO'
              AND p.id != ?
              AND (',' || COALESCE(p.fechas_solicitadas, '') || ',') LIKE '%,' || ? || ',%'
            LIMIT 1
        """, (row["vehiculo_id"], prestamo_id, fecha_prestamo))
        if cur.fetchone():
            conn.close()
            return False, "El vehiculo ya tiene un prestamo validado para esa fecha."
        cur.execute("""
            UPDATE prestamos_vehiculos
            SET estado='VALIDADO'
            WHERE id=?
        """, (prestamo_id,))
        conn.commit()
        conn.close()
        return True, "Prestamo validado."

    def marcar_prestamo_rechazado(self, prestamo_id: int, usuario_id: int) -> Tuple[bool, str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, estado
            FROM prestamos_vehiculos
            WHERE id=?
        """, (prestamo_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False, "Prestamo no encontrado."
        estado = (row["estado"] or "").upper()
        if estado == "RECHAZADO":
            conn.close()
            return False, "El prestamo ya fue rechazado."
        if estado == "VALIDADO":
            conn.close()
            return False, "El prestamo ya fue validado."
        cur.execute("""
            UPDATE prestamos_vehiculos
            SET estado='RECHAZADO'
            WHERE id=?
        """, (prestamo_id,))
        conn.commit()
        conn.close()
        return True, "Prestamo rechazado."

    def marcar_devuelto(self, movimiento_id: int, usuario_id: int) -> Tuple[bool, str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, fecha_entrega, devuelto
            FROM movimientos
            WHERE id=?
        """, (movimiento_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False, "Movimiento no encontrado."
        if row["devuelto"]:
            conn.close()
            return False, "El movimiento ya está devuelto."
        if not row["fecha_entrega"]:
            conn.close()
            return False, "No se puede devolver sin entrega registrada."
        cur.execute("""
            UPDATE movimientos
            SET fecha_devolucion=?, devuelto=1
            WHERE id=?
        """, (_hoy_iso(), movimiento_id))
        self._registrar_evento(cur, movimiento_id, usuario_id, "DEVUELTO", "")
        conn.commit()
        conn.close()
        return True, "Devolución registrada."

    def movimientos_con_alerta(self, movimientos: List[Dict], dias_alerta: int) -> List[MovimientoRow]:
        rows = []
        for mov in movimientos:
            alerta = False
            if mov.get("fecha_entrega") and not mov.get("devuelto"):
                try:
                    fecha_entrega = datetime.strptime(mov["fecha_entrega"], "%Y-%m-%d").date()
                    alerta = date.today() - fecha_entrega >= timedelta(days=dias_alerta)
                except ValueError:
                    alerta = False
            rows.append(MovimientoRow(data=mov, alerta=alerta))
        return rows

    def _registrar_evento(self, cur, movimiento_id: int, usuario_id: int, evento: str, notas: str):
        cur.execute("""
            INSERT INTO movimientos_eventos (movimiento_id, usuario_id, evento, fecha, notas)
            VALUES (?, ?, ?, ?, ?)
        """, (movimiento_id, usuario_id, evento, _hoy_iso(), notas))

    def _generar_folio(self, cur) -> str:
        ahora = datetime.now()
        fecha = ahora.strftime("%Y%m%d")
        hora = ahora.strftime("%H%M%S")
        sufijo = random.randint(0, 999)
        return f"ACTA-{fecha}-{hora}-{sufijo:03d}"
