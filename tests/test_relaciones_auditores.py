import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from scripts import actualizar_relaciones_20260908 as relaciones
from scripts.utils import AUDITOR_RUBEN_MENDEZ_CANONICO, DatabaseManager


class RelacionesAuditoresTest(unittest.TestCase):
    def test_intercambio_persiste_al_inicializar(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "inventarios.db"
            catalogos = str(Path(tmp) / "catalogos")
            DatabaseManager(str(db_path), catalogos)

            def nombres(usuario):
                with closing(sqlite3.connect(db_path)) as conn:
                    return {row[0] for row in conn.execute("""
                        SELECT a.nombre
                        FROM usuarios u
                        JOIN responsables r ON r.nombre=u.nombre
                        JOIN responsables_auditores ra ON ra.responsable_id=r.id
                        JOIN auditores a ON a.id=ra.auditor_id
                        WHERE u.usuario=?
                    """, (usuario,))}

            self.assertNotIn(AUDITOR_RUBEN_MENDEZ_CANONICO, nombres("miguel"))
            with patch.object(relaciones, "DB", db_path):
                relaciones.main()
            miguel, juan = nombres("miguel"), nombres("juan")
            self.assertIn("C.P. Vanesa Angulo Ramírez", miguel)
            self.assertNotIn("C.P. Melina Flores Peña", miguel)
            self.assertIn("C.P. Melina Flores Peña", juan)
            self.assertNotIn("C.P. Vanesa Angulo Ramírez", juan)

            DatabaseManager(str(db_path), catalogos)
            self.assertEqual(nombres("miguel"), miguel)
            self.assertEqual(nombres("juan"), juan)
            self.assertNotIn(AUDITOR_RUBEN_MENDEZ_CANONICO, nombres("miguel"))


if __name__ == "__main__":
    unittest.main()
