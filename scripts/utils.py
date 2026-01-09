"""
Utilidades y acceso a datos para Inventarios OFS.
"""

import hashlib
import logging
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("INVENTARIOS")


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
    "OFICIALIA MAYOR DE GOBIERNO",
    "SECRETARÍA DE FINANZAS",
    "SECRETARÍA DE DESARROLLO ECONOMICO",
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
    "INSTITUTO TLAXCALTECA PARA LA EDUCACIÓN DE LOS ADULTOS, ITEA",
    "ÓRGANISMO PÚBLICO DESCENTRALIZADO SALUD DE TLAXCALA",
    "PATRONATO CENTRO DE REHABILITACIÓN INTEGRAL Y ESCUELA EN TERAPIA FÍSICA Y REHABILITACIÓN",
    "PATRONATO \"LA LIBERTAD CENTRO CULTURAL DE APIZACO\"",
    "PENSIONES CIVILES DEL ESTADO DE TLAXCALA",
    "SISTEMA ESTATAL PARA EL DESARROLLO INTEGRAL DE LA FAMILIA",
    "UNIDAD DE SERVICIOS EDUCATIVOS DEL ESTADO DE TLAXCALA",
    "UNIVERSIDAD POLITÉCNICA DE TLAXCALA",
    "UNIVERSIDAD POLITÉCNICA DE TLAXCALA REGIÓN PONIENTE",
    "PODER JUDICIAL DEL ESTADO DE TLAXCALA",
    "UNIVERSIDAD TECNOLÓGICA DE TLAXCALA",
    "UNIVERSIDAD INTERCULTURAL DE TLAXCALA",
    "ARCHIVO GENERAL E HISTORICO DEL ESTADO DE TLAXCALA",
    "TRIBUNAL DE JUSTICIA ADMINISTRATIVA DEL ESTADO DE TLAXCALA",
    "UNIVERSIDAD AUTÓNOMA DE TLAXCALA",
    "COMISIÓN ESTATAL DE DERECHOS HUMANOS",
    "INSTITUTO TLAXCALTECA DE ELECCIONES",
    "INSTITUTO DE ACCESO A LA INFORMACIÓN PÚBLICA Y PROTECCIÓN DE DATOS PERSONALES DEL ESTADO DE TLAXCALA",
    "TRIBUNAL DE CONCILIACIÓN Y ARBITRAJE DEL ESTADO DE TLAXCALA",
    "TRIBUNAL ELECTORAL DE TLAXCALA",
    "CENTRO DE CONCILIACIÓN LABORAL DEL ESTADO DE TLAXCALA",
    "FISCALÍA GENERAL DE JUSTICIA DEL ESTADO DE TLAXCALA",
    "SECRETARIA EJECUTIVA DEL SISTEMA ANTICORRUPCIÓN DEL ESTADO DE TLAXCALA",
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
        self._ensure_movimientos_columns()
        self._ensure_entes_columns()
        self._seed_catalogos()
        self._seed_usuarios()
        self._seed_inventario()
        self._seed_vehiculos()
        self._seed_responsables()
        self._seed_resguardantes()
        self._seed_notificaciones()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
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
                entes TEXT NOT NULL DEFAULT 'TODOS',
                activo INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS entes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                num TEXT,
                clave TEXT UNIQUE NOT NULL,
                nombre TEXT NOT NULL,
                siglas TEXT,
                direccion TEXT,
                tipo TEXT NOT NULL,
                activo INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS inventario_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sigla TEXT UNIQUE NOT NULL,
                nombre TEXT NOT NULL,
                categoria TEXT NOT NULL,
                no_inventario TEXT,
                descripcion TEXT,
                stock_total INTEGER NOT NULL DEFAULT 0,
                stock_disponible INTEGER NOT NULL DEFAULT 0,
                activo INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS movimientos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folio TEXT UNIQUE NOT NULL,
                item_id INTEGER NOT NULL,
                usuario_id INTEGER NOT NULL,
                ente_clave TEXT NOT NULL,
                fecha_solicitud TEXT NOT NULL,
                fecha_entrega TEXT,
                fecha_devolucion TEXT,
                cantidad INTEGER NOT NULL,
                receptor_nombre TEXT NOT NULL,
                firma_recepcion TEXT,
                no_inventario TEXT,
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
                tipo_notificacion_id INTEGER,
                no_pasajeros INTEGER,
                auditores_nombres TEXT,
                ruta_destino TEXT,
                motivo_salida TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(item_id) REFERENCES inventario_items(id),
                FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
            );

            CREATE TABLE IF NOT EXISTS vehiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                placa TEXT UNIQUE NOT NULL,
                modelo TEXT NOT NULL,
                marca TEXT NOT NULL,
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

            CREATE TABLE IF NOT EXISTS notificaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE NOT NULL,
                activo INTEGER DEFAULT 1
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

            CREATE TABLE IF NOT EXISTS movimientos_pasajeros (
                movimiento_id INTEGER NOT NULL,
                usuario_id INTEGER NOT NULL,
                PRIMARY KEY (movimiento_id, usuario_id),
                FOREIGN KEY(movimiento_id) REFERENCES movimientos(id),
                FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
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
                FOREIGN KEY(usuario_id) REFERENCES usuarios(id),
                FOREIGN KEY(vehiculo_id) REFERENCES vehiculos(id)
            );
        """)
        conn.commit()
        conn.close()

    def _ensure_entes_columns(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(entes)")
        existentes = {row["name"] for row in cur.fetchall()}
        if "direccion" not in existentes:
            cur.execute("ALTER TABLE entes ADD COLUMN direccion TEXT")
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
            ("tipo_notificacion_id", "INTEGER"),
            ("no_pasajeros", "INTEGER"),
            ("auditores_nombres", "TEXT"),
            ("ruta_destino", "TEXT"),
            ("motivo_salida", "TEXT"),
        ]
        for nombre, tipo in columnas:
            if nombre not in existentes:
                cur.execute(f"ALTER TABLE movimientos ADD COLUMN {nombre} {tipo}")
        conn.commit()
        conn.close()

    def _seed_catalogos(self):
        entes = []
        municipios = []
        if not self.catalogos_dir.exists():
            logger.warning("Directorio de catálogos no encontrado: %s", self.catalogos_dir)
        else:
            try:
                from openpyxl import load_workbook
            except ImportError:
                logger.warning("openpyxl no está instalado; omitiendo carga de catálogos.")
                load_workbook = None
            if load_workbook:
                def cargar_archivo(nombre: str, tipo: str):
                    path = self.catalogos_dir / nombre
                    if not path.exists():
                        logger.warning("Catálogo no encontrado: %s", path)
                        return []

                    wb = load_workbook(path, read_only=True, data_only=True)
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

                    datos = []
                    for row in rows:
                        clave = get_val(row, ["CLAVE"])
                        nombre_val = get_val(row, ["NOMBRE"])
                        if not clave or not nombre_val:
                            continue
                        datos.append({
                            "num": get_val(row, ["NUM"]),
                            "clave": str(clave).strip(),
                            "nombre": str(nombre_val).strip(),
                            "siglas": (get_val(row, ["SIGLA"]) or "").strip(),
                            "direccion": (get_val(row, ["DIRECCION", "DOMICILIO"]) or "").strip(),
                            "tipo": tipo,
                        })
                    return datos

                entes = cargar_archivo("Estatales.xlsx", "ENTE")
                municipios = cargar_archivo("Municipales.xlsx", "MUNICIPIO")

        claves_usadas = {row["clave"] for row in entes + municipios if row.get("clave")}

        def _build_manual_rows(nombres: List[str], tipo: str) -> List[Dict]:
            rows = []
            vistos = set()
            for nombre in nombres:
                nombre_txt = str(nombre).strip()
                if not nombre_txt:
                    continue
                nombre_key = nombre_txt.upper()
                if nombre_key in vistos:
                    continue
                vistos.add(nombre_key)
                clave_base = _normalizar_clave(nombre_txt)
                clave = clave_base
                counter = 2
                while not clave or clave in claves_usadas:
                    clave = f"{clave_base}_{counter}"
                    counter += 1
                claves_usadas.add(clave)
                rows.append({
                    "num": None,
                    "clave": clave,
                    "nombre": nombre_txt,
                    "siglas": "",
                    "direccion": "",
                    "tipo": tipo,
                })
            return rows

        entes_manual = _build_manual_rows(ENTES_MANUALES, "ENTE")
        municipios_manual = _build_manual_rows(MUNICIPIOS_MANUALES, "MUNICIPIO")

        if not entes and not municipios and not entes_manual and not municipios_manual:
            return

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT clave, nombre FROM entes")
        existentes_por_nombre = {
            (row["nombre"] or "").strip().upper(): row["clave"]
            for row in cur.fetchall()
        }
        for row in entes + municipios + entes_manual + municipios_manual:
            nombre_key = (row.get("nombre") or "").strip().upper()
            if nombre_key in existentes_por_nombre:
                row["clave"] = existentes_por_nombre[nombre_key]
            cur.execute("""
                INSERT INTO entes (num, clave, nombre, siglas, direccion, tipo, activo)
                VALUES (?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(clave) DO UPDATE SET
                    num=excluded.num,
                    nombre=excluded.nombre,
                    siglas=excluded.siglas,
                    direccion=excluded.direccion,
                    tipo=excluded.tipo,
                    activo=1
            """, (row["num"], row["clave"], row["nombre"], row["siglas"], row["direccion"], row["tipo"]))
        conn.commit()
        conn.close()

    def _seed_usuarios(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM usuarios")
        if cur.fetchone()[0] > 0:
            cur.execute("SELECT COUNT(*) FROM usuarios WHERE rol='monitor'")
            if cur.fetchone()[0] == 0:
                cur.execute(
                    "INSERT INTO usuarios (nombre, usuario, clave, rol, entes) VALUES (?, ?, ?, ?, ?)",
                    ("Monitor Vehicular", "monitor", _hash_password("monitor5010"), "monitor", "TODOS"),
                )
                conn.commit()
            conn.close()
            return

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
                    entes_txt,
                ))

        if not usuarios:
            usuarios = [
                ("Administrador Inventario", "admin", _hash_password("admin5010"), "admin", "TODOS"),
                ("Usuario Auditor", "usuario", _hash_password("usuario5010"), "user", "TODOS"),
            ]
            logger.warning("Usuarios base creados; cambie las claves por seguridad.")

        cur.executemany(
            "INSERT INTO usuarios (nombre, usuario, clave, rol, entes) VALUES (?, ?, ?, ?, ?)",
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

    # -------------------------------------------------------
    # Catálogos
    # -------------------------------------------------------
    def listar_entes(self) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT num, clave, nombre, siglas, direccion, tipo
            FROM entes
            WHERE activo=1
            ORDER BY num, nombre
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def listar_usuarios(self) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre, usuario, rol
            FROM usuarios
            WHERE activo=1
            ORDER BY nombre
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

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

    def _seed_inventario(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM inventario_items")
        if cur.fetchone()[0] > 0:
            conn.close()
            return

        items = [
            (
                "XVZ-357-C",
                "Nissan Versa Sense",
                "VEHICULO",
                "INV-001",
                "Marca: NISSAN · Modelo: VERSA SENSE",
                1,
            ),
            (
                "XVZ-385-C",
                "Volkswagen Versa Sense",
                "VEHICULO",
                "INV-002",
                "Marca: VOLKSWAGEN · Modelo: VERSA SENSE",
                1,
            ),
        ]

        cur.executemany("""
            INSERT INTO inventario_items (
                sigla, nombre, categoria, no_inventario, descripcion,
                stock_total, stock_disponible
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            (sigla, nombre, categoria, no_inventario, descripcion, stock, stock)
            for sigla, nombre, categoria, no_inventario, descripcion, stock in items
        ])
        conn.commit()
        conn.close()

    def _seed_vehiculos(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM vehiculos")
        if cur.fetchone()[0] > 0:
            conn.close()
            return

        vehiculos = [
            ("XVZ-357-C", "VERSA SENSE", "NISSAN"),
            ("XVZ-385-C", "VERSA SENSE", "VOLKSWAGEN"),
            ("XBL-902-B", "JETTA", "VOLKSWAGEN"),
        ]
        cur.executemany("""
            INSERT INTO vehiculos (placa, modelo, marca, activo)
            VALUES (?, ?, ?, 1)
        """, vehiculos)
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
            ("Usuario Auditor",),
            ("Maria Rodriguez",),
            ("Jose Hernandez",),
            ("Carmen Vazquez",),
            ("Rafael Torres",),
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
            ("Diana Morales",),
            ("Hector Jimenez",),
            ("Laura Flores",),
            ("Ernesto Medina",),
            ("Ana Castillo",),
        ]
        cur.executemany("""
            INSERT INTO auditores (nombre, activo)
            VALUES (?, 1)
        """, auditores)
        conn.commit()
        conn.close()

    def _seed_notificaciones(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM notificaciones")
        if cur.fetchone()[0] > 0:
            conn.close()
            return

        notificaciones = [
            ("Notificacion oficial",),
            ("Revision de auditoria",),
        ]
        cur.executemany("""
            INSERT INTO notificaciones (nombre, activo)
            VALUES (?, 1)
        """, notificaciones)
        conn.commit()
        conn.close()

    # -------------------------------------------------------
    # Inventario
    # -------------------------------------------------------
    def listar_items(self, activos: bool = True) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        q = """
            SELECT id, sigla, nombre, categoria, no_inventario, descripcion,
                   stock_total, stock_disponible, activo
            FROM inventario_items
        """
        if activos:
            q += " WHERE activo=1"
        q += " ORDER BY categoria, sigla"
        cur.execute(q)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

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
                SELECT v.id, v.placa, v.modelo, v.marca
                FROM vehiculos v
                JOIN usuarios_vehiculos uv ON uv.vehiculo_id = v.id
                WHERE v.activo=1 AND uv.usuario_id=?
                ORDER BY v.placa
            """, (usuario_id,))
        else:
            cur.execute("""
                SELECT id, placa, modelo, marca
                FROM vehiculos
                WHERE activo=1
                ORDER BY placa
            """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

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

    def listar_notificaciones(self) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre
            FROM notificaciones
            WHERE activo=1
            ORDER BY nombre
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return data

    def crear_item(
        self,
        sigla: str,
        nombre: str,
        categoria: str,
        no_inventario: str,
        descripcion: str,
        stock_total: int,
    ) -> Tuple[bool, str]:
        if not sigla or not nombre:
            return False, "Faltan datos requeridos."
        conn = self._connect()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO inventario_items (
                    sigla, nombre, categoria, no_inventario, descripcion,
                    stock_total, stock_disponible
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                sigla.strip().upper(),
                nombre.strip(),
                categoria.strip().upper(),
                no_inventario.strip() if no_inventario else None,
                descripcion.strip() if descripcion else None,
                int(stock_total),
                int(stock_total),
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return False, "La sigla ya existe en el inventario."
        conn.close()
        return True, "Bien registrado correctamente."

    # -------------------------------------------------------
    # Movimientos
    # -------------------------------------------------------
    def crear_movimiento(
        self,
        item_id: int,
        usuario_id: int,
        ente_clave: str,
        cantidad: int,
        receptor_nombre: str,
        firma_recepcion: str,
        observaciones: str,
        resguardante_id: int,
        vehiculo_id: int,
        responsable_usuario_id: int,
        no_pasajeros: int,
        pasajeros_ids: List[int],
        ruta_destinos: List[str],
        motivo_salida: str,
        tipo_notificacion_id: int,
        fecha_solicitud: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        requeridos = [
            ("resguardante", resguardante_id),
            ("vehiculo", vehiculo_id),
            ("responsable", responsable_usuario_id),
            ("pasajeros", no_pasajeros),
            ("nombres_pasajeros", pasajeros_ids),
            ("ruta", ruta_destinos),
            ("notificacion", tipo_notificacion_id),
            ("motivo", motivo_salida),
        ]
        for clave, valor in requeridos:
            if valor is None or (isinstance(valor, str) and not valor.strip()):
                return False, {"mensaje": f"Falta el dato de {clave}."}
            if isinstance(valor, list) and not valor:
                return False, {"mensaje": f"Falta el dato de {clave}."}
        if motivo_salida not in {"Notificación de Oficio", "Revisión de Auditoría"}:
            return False, {"mensaje": "Motivo de salida no valido."}

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre
            FROM resguardantes
            WHERE id=? AND activo=1
        """, (resguardante_id,))
        resguardante = cur.fetchone()
        if not resguardante:
            conn.close()
            return False, {"mensaje": "Resguardante no encontrado."}

        cur.execute("""
            SELECT id, placa, modelo, marca
            FROM vehiculos
            WHERE id=? AND activo=1
        """, (vehiculo_id,))
        vehiculo = cur.fetchone()
        if not vehiculo:
            conn.close()
            return False, {"mensaje": "Vehiculo no encontrado."}

        cur.execute("""
            SELECT id, nombre
            FROM usuarios
            WHERE id=? AND activo=1
        """, (responsable_usuario_id,))
        responsable = cur.fetchone()
        if not responsable:
            conn.close()
            return False, {"mensaje": "Responsable no encontrado."}

        cur.execute("""
            SELECT id, nombre
            FROM notificaciones
            WHERE id=? AND activo=1
        """, (tipo_notificacion_id,))
        notificacion = cur.fetchone()
        if not notificacion:
            conn.close()
            return False, {"mensaje": "Tipo de notificacion no encontrado."}

        pasajeros_ids = [int(pid) for pid in pasajeros_ids if pid]
        if len(pasajeros_ids) != len(set(pasajeros_ids)):
            conn.close()
            return False, {"mensaje": "Pasajeros duplicados en la seleccion."}
        placeholders = ",".join(["?"] * len(pasajeros_ids))
        cur.execute(f"""
            SELECT id, nombre
            FROM usuarios
            WHERE id IN ({placeholders}) AND activo=1
            ORDER BY nombre
        """, pasajeros_ids)
        pasajeros = cur.fetchall()
        if len(pasajeros) != len(set(pasajeros_ids)):
            conn.close()
            return False, {"mensaje": "Pasajeros no encontrados."}
        if int(no_pasajeros) != len(pasajeros_ids):
            conn.close()
            return False, {"mensaje": "El numero de pasajeros no coincide."}

        destinos = [clave.strip().upper() for clave in ruta_destinos if clave and clave.strip()]
        if not destinos:
            conn.close()
            return False, {"mensaje": "Falta el dato de ruta."}
        placeholders_dest = ",".join(["?"] * len(destinos))
        cur.execute(f"""
            SELECT clave
            FROM entes
            WHERE clave IN ({placeholders_dest}) AND activo=1
        """, destinos)
        entes_validos = {row["clave"] for row in cur.fetchall()}
        if entes_validos != set(destinos):
            conn.close()
            return False, {"mensaje": "Destinos no encontrados en catalogo de entes."}
        cur.execute("""
            SELECT id, no_inventario
            FROM inventario_items
            WHERE id=? AND activo=1
        """, (item_id,))
        item = cur.fetchone()
        if not item:
            conn.close()
            return False, "Bien no encontrado."

        folio = self._generar_folio(cur)
        fecha_solicitud = _parse_date(fecha_solicitud) or _hoy_iso()
        receptor_nombre = receptor_nombre.strip() or resguardante["nombre"]
        ente_clave = ente_clave or destinos[0]

        cur.execute("""
            INSERT INTO movimientos (
                folio, item_id, usuario_id, ente_clave, fecha_solicitud,
                cantidad, receptor_nombre, firma_recepcion, no_inventario, observaciones,
                resguardante_nombre, resguardante_id, placa_unidad, marca, modelo,
                responsable_vehiculo, vehiculo_id, responsable_id, tipo_notificacion_id,
                no_pasajeros, ruta_destino, motivo_salida
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            folio,
            item_id,
            usuario_id,
            ente_clave,
            fecha_solicitud,
            int(cantidad),
            receptor_nombre.strip(),
            firma_recepcion.strip() if firma_recepcion else None,
            item["no_inventario"],
            observaciones.strip() if observaciones else None,
            resguardante["nombre"],
            resguardante["id"],
            vehiculo["placa"],
            vehiculo["marca"],
            vehiculo["modelo"],
            responsable["nombre"],
            int(vehiculo_id),
            int(responsable_usuario_id),
            notificacion["id"],
            int(no_pasajeros),
            " -> ".join(destinos),
            motivo_salida.strip(),
        ))

        movimiento_id = cur.lastrowid
        if pasajeros_ids:
            cur.executemany("""
                INSERT INTO movimientos_pasajeros (movimiento_id, usuario_id)
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

    def listar_movimientos(self, usuario_id: Optional[int] = None) -> List[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        q = """
            SELECT m.id, m.folio, m.ente_clave, m.fecha_solicitud,
                   m.fecha_entrega, m.fecha_devolucion, m.cantidad,
                   m.receptor_nombre, m.firma_recepcion, m.no_inventario,
                   m.devuelto, m.observaciones, m.resguardante_nombre,
                   COALESCE(m.placa_unidad, v.placa) AS placa_unidad,
                   COALESCE(m.marca, v.marca) AS marca,
                   COALESCE(m.modelo, v.modelo) AS modelo,
                   COALESCE(m.responsable_vehiculo, uresp.nombre) AS responsable_vehiculo,
                   m.vehiculo_id, m.responsable_id, m.tipo_notificacion_id,
                   m.no_pasajeros,
                   COALESCE((
                       SELECT group_concat(nombre, ', ')
                       FROM (
                           SELECT u2.nombre AS nombre
                           FROM movimientos_pasajeros mp
                           JOIN usuarios u2 ON u2.id = mp.usuario_id
                           WHERE mp.movimiento_id = m.id
                           ORDER BY u2.nombre
                       )
                   ), "") AS pasajeros_nombres,
                   COALESCE((
                       SELECT group_concat(destino, ' -> ')
                       FROM (
                           SELECT COALESCE(e2.nombre, md.ente_clave) AS destino
                           FROM movimientos_destinos md
                           LEFT JOIN entes e2 ON e2.clave = md.ente_clave
                           WHERE md.movimiento_id = m.id
                           ORDER BY md.orden
                       )
                   ), m.ruta_destino) AS ruta_destino,
                   m.motivo_salida,
                   e.nombre AS ente_nombre, e.siglas AS ente_siglas,
                   i.sigla, i.nombre AS item_nombre, i.categoria,
                   n.nombre AS tipo_notificacion,
                   u.nombre AS usuario_nombre
            FROM movimientos m
            JOIN inventario_items i ON i.id = m.item_id
            JOIN usuarios u ON u.id = m.usuario_id
            LEFT JOIN entes e ON e.clave = m.ente_clave
            LEFT JOIN notificaciones n ON n.id = m.tipo_notificacion_id
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
        conn.close()
        return data

    def obtener_movimiento(self, movimiento_id: int) -> Optional[Dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT m.id, m.folio, m.ente_clave, m.fecha_solicitud,
                   m.fecha_entrega, m.fecha_devolucion, m.cantidad,
                   m.receptor_nombre, m.firma_recepcion, m.no_inventario,
                   m.devuelto, m.observaciones, m.resguardante_nombre,
                   COALESCE(m.placa_unidad, v.placa) AS placa_unidad,
                   COALESCE(m.marca, v.marca) AS marca,
                   COALESCE(m.modelo, v.modelo) AS modelo,
                   COALESCE(m.responsable_vehiculo, uresp.nombre) AS responsable_vehiculo,
                   m.vehiculo_id, m.responsable_id, m.tipo_notificacion_id,
                   m.no_pasajeros,
                   COALESCE((
                       SELECT group_concat(nombre, ', ')
                       FROM (
                           SELECT u2.nombre AS nombre
                           FROM movimientos_pasajeros mp
                           JOIN usuarios u2 ON u2.id = mp.usuario_id
                           WHERE mp.movimiento_id = m.id
                           ORDER BY u2.nombre
                       )
                   ), "") AS pasajeros_nombres,
                   COALESCE((
                       SELECT group_concat(destino, ' -> ')
                       FROM (
                           SELECT COALESCE(e2.nombre, md.ente_clave) AS destino
                           FROM movimientos_destinos md
                           LEFT JOIN entes e2 ON e2.clave = md.ente_clave
                           WHERE md.movimiento_id = m.id
                           ORDER BY md.orden
                       )
                   ), m.ruta_destino) AS ruta_destino,
                   m.motivo_salida,
                   i.sigla, i.nombre AS item_nombre, i.categoria,
                   u.nombre AS usuario_nombre,
                   e.nombre AS ente_nombre, e.siglas AS ente_siglas,
                   n.nombre AS tipo_notificacion,
                   e.direccion AS ente_direccion
            FROM movimientos m
            JOIN inventario_items i ON i.id = m.item_id
            JOIN usuarios u ON u.id = m.usuario_id
            LEFT JOIN entes e ON e.clave = m.ente_clave
            LEFT JOIN notificaciones n ON n.id = m.tipo_notificacion_id
            LEFT JOIN vehiculos v ON v.id = m.vehiculo_id
            LEFT JOIN usuarios uresp ON uresp.id = m.responsable_id
            WHERE m.id=?
            LIMIT 1
        """, (movimiento_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None

    def marcar_entregado(self, movimiento_id: int, usuario_id: int) -> Tuple[bool, str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT m.id, m.cantidad, m.fecha_entrega, m.devuelto,
                   i.id AS item_id, i.stock_disponible
            FROM movimientos m
            JOIN inventario_items i ON i.id = m.item_id
            WHERE m.id=?
        """, (movimiento_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False, "Movimiento no encontrado."
        if row["fecha_entrega"]:
            conn.close()
            return False, "El movimiento ya fue entregado."
        if row["stock_disponible"] < row["cantidad"]:
            conn.close()
            return False, "No hay existencias suficientes."

        cur.execute("""
            UPDATE inventario_items
            SET stock_disponible = stock_disponible - ?
            WHERE id=?
        """, (row["cantidad"], row["item_id"]))
        cur.execute("""
            UPDATE movimientos
            SET fecha_entrega=?
            WHERE id=?
        """, (_hoy_iso(), movimiento_id))
        self._registrar_evento(cur, movimiento_id, usuario_id, "ENTREGADO", "")
        conn.commit()
        conn.close()
        return True, "Entrega registrada."

    def marcar_devuelto(self, movimiento_id: int, usuario_id: int) -> Tuple[bool, str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT m.id, m.cantidad, m.fecha_entrega, m.devuelto,
                   i.id AS item_id
            FROM movimientos m
            JOIN inventario_items i ON i.id = m.item_id
            WHERE m.id=?
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
            UPDATE inventario_items
            SET stock_disponible = stock_disponible + ?
            WHERE id=?
        """, (row["cantidad"], row["item_id"]))
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
        hoy = datetime.now().strftime("%Y%m%d")
        cur.execute("""
            SELECT COUNT(*) FROM movimientos
            WHERE fecha_solicitud=?
        """, (_hoy_iso(),))
        contador = cur.fetchone()[0] + 1
        return f"ACTA-{hoy}-{contador:04d}"
