from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))


class EditarSolicitudMonitorTest(unittest.TestCase):
    def test_monitor_actualiza_campos_impresos(self):
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
                usuario_id = conn.execute(
                    "SELECT id FROM usuarios WHERE activo=1 ORDER BY id LIMIT 1"
                ).fetchone()["id"]
                vehiculo = conn.execute("""
                    SELECT v.id, v.placa, v.marca, v.modelo, u.id AS resguardante_id, u.nombre AS resguardante
                    FROM vehiculos v
                    JOIN usuarios_vehiculos uv ON uv.vehiculo_id=v.id
                    JOIN usuarios u ON u.id=uv.usuario_id
                    WHERE v.activo=1
                    ORDER BY v.id
                    LIMIT 1
                """).fetchone()
                auditores = conn.execute(
                    "SELECT id, nombre FROM auditores WHERE activo=1 ORDER BY id LIMIT 2"
                ).fetchall()
                conn.execute("""
                    INSERT OR IGNORE INTO entes (clave, nombre, tipo, activo)
                    VALUES ('ENTE_TEST', 'Ente de prueba', 'ENTE', 1)
                """)
                ente = "ENTE_TEST"
                cur = conn.execute("""
                    INSERT INTO movimientos (
                        folio, usuario_id, ente_clave, fecha_solicitud, cantidad,
                        receptor_nombre, resguardante_nombre, resguardante_id,
                        placa_unidad, marca, modelo, responsable_vehiculo,
                        vehiculo_id, responsable_id, no_pasajeros, ruta_destino,
                        motivo_salida
                    ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """, (
                    "ACTA-TEST-EDITAR",
                    usuario_id,
                    ente,
                    "2026-08-24",
                    vehiculo["resguardante"],
                    vehiculo["resguardante"],
                    vehiculo["resguardante_id"],
                    vehiculo["placa"],
                    vehiculo["marca"],
                    vehiculo["modelo"],
                    auditores[0]["nombre"],
                    vehiculo["id"],
                    auditores[0]["id"],
                    ente,
                    "Compulsas",
                ))
                movimiento_id = cur.lastrowid
                conn.execute(
                    "INSERT INTO movimientos_auditores (movimiento_id, auditor_id) VALUES (?, ?)",
                    (movimiento_id, auditores[1]["id"]),
                )
                conn.execute(
                    "INSERT INTO movimientos_destinos (movimiento_id, ente_clave, orden) VALUES (?, ?, 1)",
                    (movimiento_id, ente),
                )
                conn.commit()

                client = app.test_client()
                with client.session_transaction() as session:
                    session.update({
                        "autenticado": True,
                        "rol": "monitor",
                        "usuario_id": usuario_id,
                    })

                url = f"/solicitudes/movimiento/{movimiento_id}/editar"
                response = client.get(url)
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Guardar cambios", response.data)

                response = client.post(url, data={
                    "fecha_consulta": "2026-08-24",
                    "vehiculo_id": str(vehiculo["id"]),
                    "fecha_solicitud": "2026-08-24",
                    "responsable_auditor_id": str(auditores[1]["id"]),
                    "ruta_destinos": [ente],
                    "motivo_salida": "Acta de Cierre",
                })
                self.assertEqual(response.status_code, 302)

                actualizado = conn.execute("""
                    SELECT responsable_id, responsable_vehiculo, no_pasajeros, motivo_salida
                    FROM movimientos
                    WHERE id=?
                """, (movimiento_id,)).fetchone()
                self.assertEqual(actualizado["responsable_id"], auditores[1]["id"])
                self.assertEqual(actualizado["responsable_vehiculo"], auditores[1]["nombre"])
                self.assertEqual(actualizado["no_pasajeros"], 0)
                self.assertEqual(actualizado["motivo_salida"], "Acta de Cierre")
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM movimientos_eventos WHERE movimiento_id=? AND evento='EDITADO'",
                        (movimiento_id,),
                    ).fetchone()[0],
                    1,
                )

                solicitante_prestamo = conn.execute(
                    "SELECT id FROM usuarios WHERE activo=1 AND id!=? ORDER BY id LIMIT 1",
                    (vehiculo["resguardante_id"],),
                ).fetchone()["id"]
                prestamo_id = conn.execute("""
                    INSERT INTO prestamos_vehiculos (
                        solicitante_id, propietario_id, vehiculo_id, fecha_solicitud,
                        estado, fechas_solicitadas, responsable_id, responsable_nombre,
                        no_pasajeros, pasajeros_ids, ruta_destino, motivo_salida
                    ) VALUES (?, ?, ?, ?, 'VALIDADO', ?, ?, ?, 0, NULL, ?, ?)
                """, (
                    solicitante_prestamo,
                    vehiculo["resguardante_id"],
                    vehiculo["id"],
                    "2026-08-25",
                    "2026-08-25",
                    auditores[0]["id"],
                    auditores[0]["nombre"],
                    ente,
                    "Compulsas",
                )).lastrowid
                conn.commit()

                response = client.post(
                    f"/solicitudes/prestamo/{prestamo_id}/editar",
                    data={
                        "fecha_consulta": "2026-08-25",
                        "vehiculo_id": str(vehiculo["id"]),
                        "fecha_solicitud": "2026-08-25",
                        "responsable_auditor_id": str(auditores[1]["id"]),
                        "ruta_destinos": [ente],
                        "motivo_salida": "Revisión de Auditoría",
                    },
                )
                self.assertEqual(response.status_code, 302)
                prestamo = conn.execute("""
                    SELECT responsable_id, responsable_nombre, motivo_salida
                    FROM prestamos_vehiculos
                    WHERE id=?
                """, (prestamo_id,)).fetchone()
                self.assertEqual(prestamo["responsable_id"], auditores[1]["id"])
                self.assertEqual(prestamo["responsable_nombre"], auditores[1]["nombre"])
                self.assertEqual(prestamo["motivo_salida"], "Revisión de Auditoría")
                conn.close()


if __name__ == "__main__":
    unittest.main()
