import sqlite3
from pathlib import Path


DB = Path(__file__).resolve().parents[1] / "inventarios.db"

COORDINADORES = {
    "C.P. Cristina Rosas de la Cruz": [
        "MAYRA ORTEGA CAMPECH",
        "IVAN XAHUENTITLA DOMINGUEZ",
        "OMAR ROMERO FLORES",
        "JULISSA KAREN FLORES PEREZ",
        "PAOLA RODRIGUEZ SANCHEZ",
    ],
    "C.P. Miguel Ángel Roldán Peña": [
        "ISAEL LOPEZ CERVANTES",
        "ARANZA SANCHEZ TREJO",
        "VANESSA ANGULO RAMIREZ",
        "EDGAR DANIEL ORDOÑEZ SALINAS",
        "ROBERTO SANCHEZ ESPINOZA",
    ],
    "C.P. Juan José Blanco Sánchez": [
        "MELINA FLORES PEÑA",
        "ALONDRA MORALES VARGAS",
        "MARGARET MICHELLE PLUMA MELENDEZ",
        "GONZALO FLORES PEREZ",
        "JONATHAN ISLAS SOSA",
        "YANETH CRUZ GEORGE",
    ],
    "C.P. Ángel Flores Licona": [
        "PATRICIA ROMANO LOPEZ",
        "BEATRIZ NETZAHUALCOYOTL NAVA",
        "DIANA ANGELICA MENDOZA CORTES",
        "ELIAZAR NAVA NAVA",
        "DAVID YAIR JUAREZ ZAINOS",
        "ENRIQUE OSORIO COTE",
    ],
}

TODOS = [
    "OSCAR IVAN",
    "GLORIA AREVALO",
    "REYNALDO",
    "ALFONSO LUIS",
    "ERICKA FLORES",
]

COINCIDENCIAS_AUDITORES = {
    "ALFONSO LUIS": "Ing. Alfonso Luis Vázquez Barrera",
    "ALONDRA MORALES VARGAS": "C.P. Alondra Morales Vargas",
    "ARANZA SANCHEZ TREJO": "C.P. Aranza Sánchez Trejo",
    "BEATRIZ NETZAHUALCOYOTL NAVA": "C.P. Beatriz Netzahualcóyotl Nava",
    "DAVID YAIR JUAREZ ZAINOS": "C.P. David Yair Juárez Zainos",
    "DIANA ANGELICA MENDOZA CORTES": "C.P. Diana Angélica Mendoza Cortés",
    "EDGAR DANIEL ORDOÑEZ SALINAS": "C.P. Edgar Daniel Ordoñez Salinas",
    "ELIAZAR NAVA NAVA": "C.P. Eliazar Nava Nava",
    "ENRIQUE OSORIO COTE": "Lic. Enrique Osorio Cote",
    "ERICKA FLORES": "Lic. Ericka Flores Flores",
    "GLORIA AREVALO": "C.P. Gloria Arévalo Gutiérrez",
    "GONZALO FLORES PEREZ": "C.P. Gonzalo Flores Pérez",
    "ISAEL LOPEZ CERVANTES": "C.P. Isael López Cervantes",
    "IVAN XAHUENTITLA DOMINGUEZ": "Lic. Iván Xahuentitla Domínguez",
    "JONATHAN ISLAS SOSA": "C.P. Jonathan Islas Sosa",
    "JULISSA KAREN FLORES PEREZ": "C.P. Julissa Karen Flores Pérez",
    "MARGARET MICHELLE PLUMA MELENDEZ": "C.P. Margaret Michelle Pluma Meléndez",
    "MAYRA ORTEGA CAMPECH": "C.P. Mayra Ortega Campech",
    "MELINA FLORES PEÑA": "C.P. Melina Flores Peña",
    "OMAR ROMERO FLORES": "C.P. Omar Romero Flores",
    "OSCAR IVAN": "C.P. Oscar Iván Ávila Sánchez",
    "PAOLA RODRIGUEZ SANCHEZ": "C.P. Paola Rodríguez Sánchez",
    "PATRICIA ROMANO LOPEZ": "C.P. Patricia Romano López",
    "REYNALDO": "C.P. Reynaldo Álvarez Teloxa",
    "ROBERTO SANCHEZ ESPINOZA": "C.P. Roberto Sánchez Espinoza",
    "VANESSA ANGULO RAMIREZ": "C.P. Vanesa Angulo Ramírez",
    "YANETH CRUZ GEORGE": "C.P. Yaneth Cruz George",
}

RELACIONES_ESPERADAS = {
    "C.P. Cristina Rosas de la Cruz": 10,
    "C.P. Miguel Ángel Roldán Peña": 10,
    "C.P. Juan José Blanco Sánchez": 11,
    "C.P. Ángel Flores Licona": 11,
}


def asegurar_id(cur, tabla: str, nombre: str) -> int:
    cur.execute(f"INSERT OR IGNORE INTO {tabla} (nombre, activo) VALUES (?, 1)", (nombre,))
    cur.execute(f"UPDATE {tabla} SET activo=1 WHERE nombre=?", (nombre,))
    cur.execute(f"SELECT id FROM {tabla} WHERE nombre=?", (nombre,))
    return cur.fetchone()[0]


def main() -> None:
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    relaciones_nuevas = []
    relaciones_eliminadas = 0
    duplicados_desactivados = 0

    for responsable, auditores in COORDINADORES.items():
        responsable_id = asegurar_id(cur, "responsables", responsable)
        deseados = [
            asegurar_id(cur, "auditores", COINCIDENCIAS_AUDITORES[auditor])
            for auditor in [*auditores, *TODOS]
        ]
        cur.execute(
            """
            DELETE FROM responsables_auditores
            WHERE responsable_id=?
              AND auditor_id NOT IN ({})
            """.format(",".join("?" for _ in deseados)),
            (responsable_id, *deseados),
        )
        relaciones_eliminadas += cur.rowcount

        for orden, (auditor, auditor_id) in enumerate(zip([*auditores, *TODOS], deseados), start=1):
            auditor_nombre = COINCIDENCIAS_AUDITORES[auditor]
            cur.execute(
                """
                INSERT OR IGNORE INTO responsables_auditores
                    (responsable_id, auditor_id, orden)
                VALUES (?, ?, ?)
                """,
                (responsable_id, auditor_id, orden),
            )
            if cur.rowcount:
                relaciones_nuevas.append((responsable, auditor_nombre))

    for auditor, auditor_nombre in COINCIDENCIAS_AUDITORES.items():
        if auditor == auditor_nombre:
            continue
        cur.execute("UPDATE auditores SET activo=0 WHERE nombre=? AND activo != 0", (auditor,))
        duplicados_desactivados += cur.rowcount

    duplicados = cur.execute(
        """
        SELECT responsable_id, auditor_id, COUNT(*)
        FROM responsables_auditores
        GROUP BY responsable_id, auditor_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    assert not duplicados, duplicados

    faltantes = []
    for responsable, auditores in COORDINADORES.items():
        esperados = [*auditores, *TODOS]
        for auditor in esperados:
            auditor_nombre = COINCIDENCIAS_AUDITORES[auditor]
            existe = cur.execute(
                """
                SELECT 1
                FROM responsables r
                JOIN responsables_auditores ra ON ra.responsable_id = r.id
                JOIN auditores a ON a.id = ra.auditor_id
                WHERE r.nombre=? AND a.nombre=?
                """,
                (responsable, auditor_nombre),
            ).fetchone()
            if not existe:
                faltantes.append((responsable, auditor_nombre))
    assert not faltantes, faltantes

    totales_responsables = dict(
        cur.execute(
            """
            SELECT r.nombre, COUNT(*)
            FROM responsables r
            JOIN responsables_auditores ra ON ra.responsable_id = r.id
            WHERE r.nombre IN ({})
            GROUP BY r.nombre
            """.format(",".join("?" for _ in RELACIONES_ESPERADAS)),
            tuple(RELACIONES_ESPERADAS),
        ).fetchall()
    )
    assert totales_responsables == RELACIONES_ESPERADAS, totales_responsables

    totales_todos = dict(
        cur.execute(
            """
            SELECT a.nombre, COUNT(*)
            FROM auditores a
            JOIN responsables_auditores ra ON ra.auditor_id = a.id
            JOIN responsables r ON r.id = ra.responsable_id
            WHERE a.nombre IN ({})
              AND r.nombre IN ({})
            GROUP BY a.nombre
            """.format(
                ",".join("?" for _ in TODOS),
                ",".join("?" for _ in RELACIONES_ESPERADAS),
            ),
            (
                *(COINCIDENCIAS_AUDITORES[auditor] for auditor in TODOS),
                *RELACIONES_ESPERADAS,
            ),
        ).fetchall()
    )
    esperados_todos = {
        COINCIDENCIAS_AUDITORES[auditor]: len(COORDINADORES)
        for auditor in TODOS
    }
    assert totales_todos == esperados_todos, totales_todos

    conn.commit()
    conn.close()

    print(f"Relaciones eliminadas: {relaciones_eliminadas}")
    print(f"Relaciones agregadas: {len(relaciones_nuevas)}")
    for responsable, auditor in relaciones_nuevas:
        print(f"  + {responsable} -> {auditor}")
    print(f"Duplicados desactivados: {duplicados_desactivados}")


if __name__ == "__main__":
    main()
