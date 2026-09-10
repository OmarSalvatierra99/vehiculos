import html
import re
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import app as vehiculos
from scripts.utils import DatabaseManager, _hash_password, normalizar_rol


class RecordatoriosTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = str(Path(self.tmp.name) / "inventarios.db")
        config = SimpleNamespace(
            TESTING=True, SECRET_KEY="recordatorios-test", DEBUG=True,
            INVENTARIOS_DB=self.db_path, CATALOGOS_DIR=self.tmp.name,
            LOG_FILE=str(Path(self.tmp.name) / "app.log"), LOG_LEVEL="ERROR",
        )
        with patch.object(vehiculos, "get_config", return_value=config):
            self.app = vehiculos.create_app("testing")
        self.client = self.app.test_client()
        self.db = DatabaseManager(self.db_path, self.tmp.name)
        self.conn = sqlite3.connect(self.db_path)
        self.addCleanup(self.conn.close)
        self.roles = [" ADMIN ", "administrador", "Gestor", "MONITOR"]
        self.admin_ids = []
        for index, rol in enumerate(self.roles):
            self.admin_ids.append(self.conn.execute("""
                INSERT INTO usuarios (nombre, usuario, clave, rol)
                VALUES (?, ?, ?, ?)
            """, (f"Administrador {index}", f"admin{index}", _hash_password("prueba"), rol)).lastrowid)
        self.username = "ana & pérez?"
        self.nombre = "C.P. Ana Pérez & Hernández"
        self.user_id = self.conn.execute("""
            INSERT INTO usuarios (nombre, usuario, clave, rol)
            VALUES (?, ?, ?, 'usuario')
        """, (self.nombre, self.username, _hash_password("prueba"))).lastrowid
        self.inactive_id = self.conn.execute("""
            INSERT INTO usuarios (nombre, usuario, clave, rol, activo)
            VALUES ('Inactivo', 'inactivo', 'test', 'user', 0)
        """).lastrowid
        self.conn.commit()
        self.url = f"/configuracion/recordatorios/{self.user_id}"

    def login_session(self, rol="monitor"):
        with self.client.session_transaction() as session:
            session.clear()
            session.update(autenticado=True, rol=rol, usuario_id=self.admin_ids[0], nombre="Prueba")

    def token(self):
        self.assertEqual(self.client.get("/configuracion/recordatorios").status_code, 200)
        with self.client.session_transaction() as session:
            return session["csrf_recordatorios"]

    def phone(self):
        return self.conn.execute(
            "SELECT telefono_whatsapp FROM usuarios WHERE id=?", (self.user_id,),
        ).fetchone()[0]

    def test_roles_login_sesiones_y_capacidades_compartidas(self):
        for index, rol in enumerate(self.roles):
            with self.subTest(rol=rol):
                self.assertEqual(normalizar_rol(rol), "monitor")
                self.client = self.app.test_client()
                response = self.client.post("/", data={"usuario": f"admin{index}", "clave": "prueba"})
                self.assertEqual(response.location, "/admin")
                with self.client.session_transaction() as session:
                    self.assertEqual(session["rol"], "monitor")
                    session["rol"] = rol  # Una cookie emitida antes del cambio.
                response = self.client.get("/admin")
                self.assertEqual(response.status_code, 200)
                for label in ("Administrador", "Recordatorios", "Alta de emergencia", "Reasignacion", "Registrar vehículo"):
                    self.assertIn(label, response.text)
                self.assertEqual(self.client.get("/configuracion/recordatorios").status_code, 200)
                response = self.client.post("/inventario/nuevo", data={
                    "placa": f"PRUEBA-{index}", "marca": "Marca", "modelo": "2026", "categoria": "Financiero",
                })
                self.assertEqual(response.location, "/admin")
                self.assertIsNotNone(self.conn.execute("SELECT id FROM vehiculos WHERE placa=?", (f"PRUEBA-{index}",)).fetchone())
                self.assertEqual(self.client.get("/reporte-diario").status_code, 200)
        for rol in ("user", "usuario", "desconocido", None):
            self.assertEqual(normalizar_rol(rol), "user")
        self.client = self.app.test_client()
        self.assertEqual(self.client.post("/", data={"usuario": self.username, "clave": "prueba"}).location, "/dashboard")

    def test_acceso_csrf_y_privacidad(self):
        self.assertEqual(self.client.get("/configuracion/recordatorios").location, "/")
        self.assertEqual(self.client.post(self.url).location, "/")
        self.login_session("usuario")
        self.assertEqual(self.client.get("/configuracion/recordatorios").status_code, 403)
        self.assertEqual(self.client.post(self.url).status_code, 403)
        self.db.guardar_telefono_whatsapp(self.user_id, "2461234567")
        dashboard = self.client.get("/dashboard").text
        self.assertNotIn("Recordatorios", dashboard)
        self.assertNotIn("2461234567", dashboard)
        self.assertNotIn("telefono_whatsapp", self.db.get_usuario(self.username, "prueba"))
        self.assertNotIn("telefono_whatsapp", self.db.get_usuario_por_username(self.username))
        self.login_session()
        for token in ("", "incorrecto", "ñ"):
            self.assertEqual(self.client.post(self.url, data={"csrf_token": token, "telefono_whatsapp": ""}).status_code, 400)
        token = self.token()
        self.assertEqual(self.client.post(self.url, data={"csrf_token": "otro", "telefono_whatsapp": ""}).status_code, 400)
        self.login_session()
        self.token()
        self.assertEqual(self.client.post(self.url, data={"csrf_token": token, "telefono_whatsapp": ""}).status_code, 400)
        self.assertEqual(self.phone(), "+522461234567")

    def test_guardado_formatos_enlaces_y_eliminacion(self):
        self.login_session()
        token = self.token()
        for numero in ("2461234567", "246 123-4567", "+52 246 123 4567", "522461234567", "+5212461234567", "5212461234567", "00522461234567"):
            with self.subTest(numero=numero):
                response = self.client.post(self.url, data={"csrf_token": token, "telefono_whatsapp": numero})
                self.assertEqual(response.status_code, 302)
                self.assertEqual(self.phone(), "+522461234567")
        DatabaseManager(self.db_path, self.tmp.name)
        self.assertEqual(self.phone(), "+522461234567")
        with patch.object(vehiculos, "datetime", wraps=datetime) as reloj:
            reloj.now.side_effect = datetime.fromisoformat("2026-09-11T23:00:00+00:00").astimezone
            response = self.client.get("/configuracion/recordatorios")
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertNotIn("Probar número", response.text)
        urls = [html.unescape(url) for url in re.findall(r'data-whatsapp-url="([^"]+)"', response.text)]
        self.assertEqual(urlsplit(urls[0]).path, "/5212461234567")
        self.assertEqual(parse_qs(urlsplit(urls[0]).query)["text"], [
            f"Buenas tardes, {self.nombre}.\n\n"
            "Se le recuerda solicitar un vehículo para el día lunes 14 de septiembre del 2026 en la plataforma:\n"
            "https://vehiculos.omar-xyz.shop\n\n"
            "Gracias por su atención.",
        ])
        self.assertNotIn(self.username, parse_qs(urlsplit(urls[0]).query)["text"][0])
        for numero in ("123", "24612345678", "+12461234567", "+52522461234567", "246123456x", "２４６１２３４５６７", "---", '<script>"'):
            with self.subTest(numero=numero):
                response = self.client.post(self.url, data={"csrf_token": token, "telefono_whatsapp": numero})
                self.assertEqual(response.status_code, 400)
                self.assertIn('aria-invalid="true"', response.text)
                self.assertEqual(self.phone(), "+522461234567")
        response = self.client.post(self.url, data={"csrf_token": token, "telefono_whatsapp": " "}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(self.phone())
        self.assertNotIn("https://wa.me/", response.text)

    def test_fecha_laboral_y_vista_previa_se_recalculan_en_hora_de_mexico(self):
        self.login_session()
        self.db.guardar_telefono_whatsapp(self.user_id, "2461234567")
        casos = [
            ("2026-09-07T23:00:00+00:00", "martes 08 de septiembre del 2026"),
            ("2026-09-08T23:00:00+00:00", "miércoles 09 de septiembre del 2026"),
            ("2026-09-09T23:00:00+00:00", "jueves 10 de septiembre del 2026"),
            ("2026-09-10T23:00:00+00:00", "viernes 11 de septiembre del 2026"),
            ("2026-09-11T23:00:00+00:00", "lunes 14 de septiembre del 2026"),
            ("2026-09-12T23:00:00+00:00", "lunes 14 de septiembre del 2026"),
            ("2026-09-13T23:00:00+00:00", "lunes 14 de septiembre del 2026"),
            ("2026-07-31T23:00:00+00:00", "lunes 03 de agosto del 2026"),
            ("2026-12-31T23:00:00+00:00", "viernes 01 de enero del 2027"),
            ("2027-12-31T23:00:00+00:00", "lunes 03 de enero del 2028"),
            ("2028-01-01T23:00:00+00:00", "lunes 03 de enero del 2028"),
            ("2028-01-02T23:00:00+00:00", "lunes 03 de enero del 2028"),
            # En UTC ya es viernes; en Ciudad de México todavía es jueves.
            ("2026-09-11T02:00:00+00:00", "viernes 11 de septiembre del 2026"),
        ]
        for instante, fecha in casos:
            with self.subTest(instante=instante), patch.object(vehiculos, "datetime", wraps=datetime) as reloj:
                reloj.now.side_effect = datetime.fromisoformat(instante).astimezone
                response = self.client.get("/configuracion/recordatorios")
                self.assertEqual(response.status_code, 200)
                enlace = html.unescape(re.search(r'data-whatsapp-url="(https://wa.me/[^\"]+\?[^\"]+)"', response.text)[1])
                mensaje = parse_qs(urlsplit(enlace).query)["text"][0]
                self.assertEqual(mensaje, (
                    f"Buenas tardes, {self.nombre}.\n\n"
                    f"Se le recuerda solicitar un vehículo para el día {fecha} en la plataforma:\n"
                    "https://vehiculos.omar-xyz.shop\n\n"
                    "Gracias por su atención."
                ))
                html_text = html.unescape(response.text)
                self.assertIn(f"Fecha automática: {fecha}", html_text)
                self.assertIn(f'data-fecha-recordatorio="{fecha}"', html_text)
                editor = re.search(r'<textarea id="mensaje-recordatorio" rows="10" data-recordatorio-mensaje>(.*?)</textarea>', html_text, re.S)[1]
                self.assertNotIn(fecha, editor)
                self.assertIn("Buenas tardes, {nombre completo}.", editor)
                self.assertIn("Se le recuerda solicitar un vehículo para el día {fecha} en la plataforma:", editor)

    def test_fecha_larga_es_conserva_tildes(self):
        self.assertEqual(vehiculos._fecha_larga_es("2026-09-09"), "Miércoles 09 de Septiembre del 2026")
        self.assertEqual(vehiculos._fecha_larga_es("2026-09-12"), "Sábado 12 de Septiembre del 2026")

    def test_destinatarios_y_resguardantes_excluyen_administradores(self):
        excluded = set(self.admin_ids + [self.inactive_id])
        self.assertFalse(excluded & {u["id"] for u in self.db.listar_usuarios_recordatorios()})
        self.assertFalse(excluded & {u["id"] for u in self.db.listar_usuarios_resguardo()})
        vehicle = self.conn.execute("SELECT id FROM vehiculos WHERE categoria='Financiero' LIMIT 1").fetchone()[0]
        self.login_session()
        token = self.token()
        for usuario_id in [*excluded, 999999]:
            with self.subTest(usuario_id=usuario_id):
                response = self.client.post(f"/configuracion/recordatorios/{usuario_id}", data={
                    "csrf_token": token, "telefono_whatsapp": "2461234567",
                })
                self.assertEqual(response.status_code, 404)
                self.assertFalse(self.db.reasignar_vehiculo(vehicle, usuario_id, "Financiero")[0])

    def test_recordatorios_viven_como_pestana_admin(self):
        self.login_session()
        response = self.client.get("/admin?tab=recordatorios")
        self.assertEqual(response.status_code, 200)
        self.assertIn('data-monitor-tab="recordatorios"', response.text)
        self.assertIn('data-monitor-panel="recordatorios"', response.text)
        self.assertIn('data-recordatorio-mensaje', response.text)
        self.assertNotIn('class="monitor-tab" href="/configuracion/recordatorios"', response.text)
        self.assertIn('name="origen" value="admin"', response.text)
        with self.client.session_transaction() as session:
            token = session["csrf_recordatorios"]

        response = self.client.post(self.url, data={
            "csrf_token": token,
            "telefono_whatsapp": "2461234567",
            "origen": "admin",
        })
        self.assertEqual(response.location, "/admin?tab=recordatorios")

        response = self.client.post("/configuracion/credenciales", data={
            "csrf_token": token,
            "usuario_id": self.user_id,
            "confirmar_restablecimiento": "1",
            "origen": "admin",
        })
        self.assertEqual(response.status_code, 200)
        self.assertIn('id="monitor-panel-recordatorios"', response.text)
        self.assertIn('href="/admin?tab=recordatorios"', response.text)

    def test_migracion_preserva_datos_y_es_repetible(self):
        path = str(Path(self.tmp.name) / "legacy.db")
        with closing(sqlite3.connect(path)) as conn:
            conn.execute("CREATE TABLE usuarios (id INTEGER PRIMARY KEY, nombre TEXT, usuario TEXT, clave TEXT, rol TEXT, puesto TEXT, entes TEXT, activo INTEGER)")
            conn.execute("INSERT INTO usuarios VALUES (7, 'Nombre', 'acceso', 'hash', 'ADMIN', 'Puesto', 'TODOS', 1)")
            original = conn.execute("SELECT * FROM usuarios").fetchall()
            conn.commit()
        manager = DatabaseManager.__new__(DatabaseManager)
        manager.db_path = path
        manager._ensure_usuarios_columns()
        manager._ensure_usuarios_columns()
        with closing(sqlite3.connect(path)) as conn:
            self.assertEqual(conn.execute("SELECT id, nombre, usuario, clave, rol, puesto, entes, activo FROM usuarios").fetchall(), original)
            self.assertEqual(conn.execute("SELECT telefono_whatsapp FROM usuarios").fetchone(), (None,))
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_credenciales_requieren_admin_csrf_confirmacion_y_telefono(self):
        url = "/configuracion/credenciales"
        data = {"usuario_id": self.user_id, "confirmar_restablecimiento": "1"}
        self.assertEqual(self.client.post(url, data=data).location, "/")
        self.login_session("user")
        self.assertEqual(self.client.post(url, data=data).status_code, 403)
        self.login_session()
        self.assertEqual(self.client.post(url, data=data).status_code, 400)
        token = self.token()
        data["csrf_token"] = token
        self.assertEqual(self.client.post(url, data=data).status_code, 400)
        self.db.guardar_telefono_whatsapp(self.user_id, "2461234567")
        self.assertEqual(self.client.post(url, data={**data, "confirmar_restablecimiento": ""}).status_code, 400)
        for usuario_id in [*self.admin_ids, self.inactive_id, 999999]:
            self.assertEqual(self.client.post(url, data={**data, "usuario_id": usuario_id}).status_code, 404)
        self.assertIsNotNone(self.db.get_usuario(self.username, "prueba"))

    def test_credenciales_preparan_mensaje_sin_cambiar_clave(self):
        self.login_session("ADMIN")
        self.db.guardar_telefono_whatsapp(self.user_id, "2461234567")
        hash_original = self.conn.execute("SELECT clave FROM usuarios WHERE id=?", (self.user_id,)).fetchone()[0]
        data = {"usuario_id": self.user_id, "confirmar_restablecimiento": "1", "csrf_token": self.token()}
        response = self.client.post("/configuracion/credenciales", data=data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        enlace = html.unescape(re.search(r'href="(https://wa.me/[^"]+)"', response.text)[1])
        mensaje = parse_qs(urlsplit(enlace).query)["text"][0]
        self.assertEqual(urlsplit(enlace).path, "/5212461234567")
        self.assertEqual(mensaje.splitlines()[0], f"Usuario: {self.username}")
        self.assertEqual(len(mensaje.splitlines()), 2)
        self.assertEqual(mensaje.splitlines()[1], "Contraseña: ")
        self.assertIsNotNone(self.db.get_usuario(self.username, "prueba"))
        hash_guardado = self.conn.execute("SELECT clave FROM usuarios WHERE id=?", (self.user_id,)).fetchone()[0]
        self.assertEqual(hash_guardado, hash_original)
        with self.client.session_transaction() as session:
            self.assertNotIn("prueba", str(dict(session)))
        self.assertNotIn("prueba", response.headers.get("Set-Cookie", ""))
        self.assertEqual(self.client.post("/configuracion/credenciales", data=data).status_code, 200)
        self.assertIsNotNone(self.db.get_usuario(self.username, "prueba"))
        self.assertEqual(self.client.get("/configuracion/credenciales").status_code, 405)


if __name__ == "__main__":
    unittest.main()
