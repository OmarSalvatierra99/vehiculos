import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import _vehiculo_frecuente


PROJECT_DIR = Path(__file__).resolve().parents[1]


class VehiculoFrecuenteTest(unittest.TestCase):
    def test_usa_historial_valido_y_disponibilidad(self):
        movimientos = [
            {"vehiculo_id": 13, "rechazado": 0},
            {"vehiculo_id": 13, "rechazado": 0},
            {"vehiculo_id": 10, "rechazado": 0},
            {"vehiculo_id": 13, "rechazado": 1},
        ]
        disponible = {"id": 13, "placa": "XXK-741-D"}

        self.assertEqual(
            _vehiculo_frecuente(movimientos, [disponible]),
            {**disponible, "pedidos": 2},
        )
        self.assertIsNone(_vehiculo_frecuente(movimientos, [{"id": 10}]))

    def test_monitor_agrega_y_valida_movimiento_rapido(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "inventarios.db"
            env = {
                "FLASK_ENV": "testing",
                "INVENTARIOS_DB": str(db_path),
                "CATALOGOS_DIR": str(PROJECT_DIR / "catalogos"),
                "LOG_FILE": str(Path(tmp) / "app.log"),
            }
            with patch.dict(os.environ, env, clear=False):
                sys.modules.pop("config", None)
                sys.modules.pop("app", None)

                from app import create_app

                app = create_app("testing")
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                conn.executemany("""
                    INSERT OR IGNORE INTO usuarios (nombre, usuario, clave, rol, entes)
                    VALUES (?, ?, 'test', ?, 'TODOS')
                """, [
                    ("Mike", "mike", "user"),
                    ("Monitor Vehicular", "monitor", "monitor"),
                ])
                mike = conn.execute(
                    "SELECT id FROM usuarios WHERE lower(usuario)='mike'"
                ).fetchone()
                monitor = conn.execute(
                    "SELECT id FROM usuarios WHERE lower(rol)='monitor' ORDER BY id LIMIT 1"
                ).fetchone()
                vehiculo = conn.execute("""
                    SELECT v.id, v.placa, v.marca, v.modelo,
                           u.id AS resguardante_id, u.nombre AS resguardante
                    FROM vehiculos v
                    JOIN usuarios_vehiculos uv ON uv.vehiculo_id=v.id
                    JOIN usuarios u ON u.id=uv.usuario_id
                    WHERE v.activo=1
                    ORDER BY v.id
                    LIMIT 1
                """).fetchone()
                auditor = conn.execute(
                    "SELECT id, nombre FROM auditores WHERE activo=1 ORDER BY id LIMIT 1"
                ).fetchone()
                conn.execute("""
                    INSERT OR IGNORE INTO entes (clave, nombre, tipo, activo)
                    VALUES ('ENTE_RAPIDO', 'Ente para alta rapida', 'ENTE', 1)
                """)
                ente = "ENTE_RAPIDO"
                referencia_id = conn.execute("""
                    INSERT INTO movimientos (
                        folio, usuario_id, ente_clave, fecha_solicitud, fecha_entrega,
                        cantidad, receptor_nombre, resguardante_nombre, resguardante_id,
                        placa_unidad, marca, modelo, responsable_vehiculo, vehiculo_id,
                        responsable_id, no_pasajeros, ruta_destino, motivo_salida
                    ) VALUES (
                        'ACTA-TEST-RAPIDO', ?, ?, '2026-08-21', '2026-08-21',
                        1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 'Compulsas'
                    )
                """, (
                    mike["id"],
                    ente,
                    vehiculo["resguardante"],
                    vehiculo["resguardante"],
                    vehiculo["resguardante_id"],
                    vehiculo["placa"],
                    vehiculo["marca"],
                    vehiculo["modelo"],
                    auditor["nombre"],
                    vehiculo["id"],
                    auditor["id"],
                    ente,
                )).lastrowid
                conn.execute(
                    "INSERT INTO movimientos_destinos (movimiento_id, ente_clave, orden) VALUES (?, ?, 1)",
                    (referencia_id, ente),
                )
                conn.commit()

                client = app.test_client()
                with client.session_transaction() as session:
                    session.update({
                        "autenticado": True,
                        "rol": "monitor",
                        "usuario_id": monitor["id"],
                        "usuario": "monitor",
                    })

                fecha = "2026-08-25"
                response = client.get(f"/admin?fecha={fecha}")
                self.assertIn(b"Agregar y validar movimiento de Mike", response.data)

                response = client.post(
                    "/movimientos/rapido/mike",
                    data={"fecha": fecha},
                )
                self.assertEqual(response.status_code, 302)

                creado = conn.execute("""
                    SELECT id, vehiculo_id, fecha_entrega, responsable_id, observaciones
                    FROM movimientos
                    WHERE usuario_id=? AND fecha_solicitud=? AND id!=?
                """, (mike["id"], fecha, referencia_id)).fetchone()
                self.assertEqual(creado["vehiculo_id"], vehiculo["id"])
                self.assertEqual(creado["responsable_id"], auditor["id"])
                self.assertIsNotNone(creado["fecha_entrega"])
                self.assertIn("Alta rapida validada por monitor", creado["observaciones"])
                self.assertEqual(
                    conn.execute("""
                        SELECT COUNT(*)
                        FROM movimientos_eventos
                        WHERE movimiento_id=? AND usuario_id=? AND evento='ENTREGADO'
                    """, (creado["id"], monitor["id"])).fetchone()[0],
                    1,
                )

                response = client.get(f"/reporte-diario?fecha={fecha}")
                self.assertIn(vehiculo["placa"].encode(), response.data)

                with client.session_transaction() as session:
                    session["rol"] = "admin"
                response = client.get(f"/admin?fecha={fecha}")
                self.assertIn(b"Agregar y validar movimiento de Mike", response.data)
                total_antes = conn.execute(
                    "SELECT COUNT(*) FROM movimientos WHERE usuario_id=?",
                    (mike["id"],),
                ).fetchone()[0]
                response = client.post(
                    "/movimientos/rapido/mike",
                    data={"fecha": "2026-08-26"},
                )
                self.assertEqual(response.status_code, 302)
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM movimientos WHERE usuario_id=?",
                        (mike["id"],),
                    ).fetchone()[0],
                    total_antes + 1,
                )
                conn.close()


if __name__ == "__main__":
    unittest.main()
