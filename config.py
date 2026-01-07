"""
Inventarios - Configuracion de la aplicacion
=============================================
Configuracion centralizada para diferentes entornos.
"""

import os
from typing import Final


class Config:
    """Configuracion base de la aplicacion."""

    SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")

    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "5010"))
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "log/app.log")

    INVENTARIOS_DB: str = os.getenv("INVENTARIOS_DB", "inventarios.db")
    CATALOGOS_DIR: str = os.getenv(
        "CATALOGOS_DIR",
        os.path.join(os.path.dirname(__file__), "catalogos"),
    )

    SESSION_COOKIE_SECURE: bool = True
    SESSION_COOKIE_HTTPONLY: bool = True
    SESSION_COOKIE_SAMESITE: str = "Lax"
    PERMANENT_SESSION_LIFETIME: int = 3600

    ALERTA_DIAS_NO_DEVUELTO: Final[int] = 7


class DevelopmentConfig(Config):
    """Configuracion para desarrollo."""

    DEBUG = True
    SESSION_COOKIE_SECURE = False


class ProductionConfig(Config):
    """Configuracion para produccion."""

    DEBUG = False


class TestingConfig(Config):
    """Configuracion para testing."""

    TESTING = True
    DEBUG = True


config_by_name = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "testing": TestingConfig,
    "default": DevelopmentConfig,
}


def get_config(config_name: str = None) -> Config:
    """Obtiene la configuracion segun el nombre del entorno."""
    if config_name is None:
        config_name = os.getenv("FLASK_ENV", "default")

    return config_by_name.get(config_name, DevelopmentConfig)
