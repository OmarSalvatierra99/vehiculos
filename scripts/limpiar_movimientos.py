"""
Limpia historial de movimientos y reportes en la base de datos.
"""

import argparse
import sqlite3
from typing import Iterable

from config import get_config


def _execute_many(cur: sqlite3.Cursor, statements: Iterable[str]) -> None:
    for stmt in statements:
        cur.execute(stmt)


def limpiar_movimientos(db_path: str, reset_stock: bool = False) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    _execute_many(cur, (
        "DELETE FROM movimientos_eventos",
        "DELETE FROM movimientos_pasajeros",
        "DELETE FROM movimientos_destinos",
        "DELETE FROM movimientos_auditores",
        "DELETE FROM movimientos",
    ))
    if reset_stock:
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
    args = parser.parse_args()

    cfg = get_config()
    db_path = args.db or cfg.INVENTARIOS_DB
    limpiar_movimientos(db_path, reset_stock=args.reset_stock)
    print("Historial de movimientos limpiado.")
    if args.reset_stock:
        print("Stock disponible restablecido al total.")


if __name__ == "__main__":
    main()
