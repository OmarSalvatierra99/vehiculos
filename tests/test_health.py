from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))


class HealthEndpointTest(unittest.TestCase):
    def test_health_endpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = {
                "FLASK_ENV": "testing",
                "INVENTARIOS_DB": str(Path(tmp) / "inventarios.db"),
                "CATALOGOS_DIR": str(PROJECT_DIR / "catalogos"),
                "LOG_FILE": str(Path(tmp) / "app.log"),
            }
            with patch.dict(os.environ, env, clear=False):
                sys.modules.pop("config", None)
                sys.modules.pop("app", None)

                from app import create_app

                app = create_app("testing")
                response = app.test_client().get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "healthy")


if __name__ == "__main__":
    unittest.main()
