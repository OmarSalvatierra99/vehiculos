"""
Limpia historial de movimientos y registros asociados en la base de datos.
"""

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import get_config  # noqa: E402


def _execute_many(cur: sqlite3.Cursor, statements: Iterable[str]) -> None:
    for stmt in statements:
        cur.execute(stmt)


def _tables_present(cur: sqlite3.Cursor) -> set:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in cur.fetchall()}


def limpiar_movimientos(
    db_path: str,
    reset_stock: bool = False,
    limpiar_vehiculos: bool = False,
) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    existentes = _tables_present(cur)
    statements = [
        "DELETE FROM movimientos_eventos",
        "DELETE FROM movimientos_destinos",
        "DELETE FROM movimientos_auditores",
        "DELETE FROM movimientos",
    ]
    if limpiar_vehiculos:
        statements.extend([
            "DELETE FROM prestamos_vehiculos",
            "DELETE FROM usuarios_vehiculos",
            "DELETE FROM vehiculos",
        ])
    _execute_many(cur, (stmt for stmt in statements if stmt.split()[-1] in existentes))
    if reset_stock and "inventario_items" in existentes:
        cur.execute("UPDATE inventario_items SET stock_disponible = stock_total")
    conn.commit()
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Limpia el historial de movimientos y reportes.",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Ruta a la base de datos SQLite. Usa config por defecto si se omite.",
    )
    parser.add_argument(
        "--reset-stock",
        action="store_true",
        help="Restablece el stock disponible al total tras limpiar movimientos.",
    )
    parser.add_argument(
        "--limpiar-vehiculos",
        action="store_true",
        help="Elimina vehiculos y relaciones asociadas (prestamos y asignaciones).",
    )
    args = parser.parse_args()

    cfg = get_config()
    db_path = args.db or cfg.INVENTARIOS_DB
    limpiar_movimientos(
        db_path,
        reset_stock=args.reset_stock,
        limpiar_vehiculos=args.limpiar_vehiculos,
    )
    print("Historial de movimientos limpiado.")
    if args.reset_stock:
        print("Stock disponible restablecido al total.")
    if args.limpiar_vehiculos:
        print("Vehiculos y relaciones asociadas eliminados.")


if __name__ == "__main__":
    main()
