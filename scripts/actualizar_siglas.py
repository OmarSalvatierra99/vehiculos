#!/usr/bin/env python3
"""
Script para actualizar las siglas de los entes en la base de datos.
"""

import sqlite3
import sys
from pathlib import Path

# Mapeo de nombres de entes a sus siglas
SIGLAS_ENTES = {
    "PODER EJECUTIVO DEL ESTADO DE TLAXCALA": "EJECUTIVO",
    "DESPACHO DE LA GOBERNADORA": "DG",
    "SECRETARÍA DE LA FUNCIÓN PÚBLICA": "SFP",
    "SECRETARÍA DE IMPULSO AGROPECUARIO": "SIA",
    "COORDINACIÓN DE COMUNICACIÓN": "CCOM",
    "SECRETARÍA DE MEDIO AMBIENTE": "SMA",
    "SECRETARÍA DE CULTURA": "SC",
    "SECRETARÍA DE LAS MUJERES": "SMET",
    "SECRETARÍA DE ORDENAMIENTO TERRITORIAL Y VIVIENDA": "SOTyV",
    "SECRETARÍA DE SEGURIDAD CIUDADANA": "SSC",
    "COORDINACIÓN GENERAL DE PLANEACIÓN E INVERSIÓN": "CGPI",
    "SECRETARÍA DE BIENESTAR": "SB",
    "SECRETARÍA DE GOBIERNO": "SEGOB",
    "SECRETARÍA DE TRABAJO Y COMPETITIVIDAD": "STyC",
    "CONSEJERÍA JURÍDICA DEL EJECUTIVO": "CJE",
    "COORDINACIÓN ESTATAL DE PROTECCIÓN CIVIL": "CEPC",
    "SECRETARIADO EJECUTIVO DEL SISTEMA ESTATAL DE SEGURIDAD PÚBLICA": "SESESP",
    "INSTITUTO TLAXCALTECA DE DESARROLLO TAURINO": "ITDT",
    "INSTITUTO TLAXCALTECA DE ASISTENCIA ESPECIALIZADA A LA SALUD": "ITAES",
    "COMISIÓN ESTATAL DE ARBITRAJE MÉDICO": "CEAM",
    "CASA DE LAS ARTESANÍAS DE TLAXCALA": "CAT",
    "PROCURADURÍA DE PROTECCIÓN AL AMBIENTE DEL ESTADO DE TLAXCALA": "PROPAET",
    "INSTITUTO DE FAUNA SILVESTRE PARA EL ESTADO DE TLAXCALA": "IFAST",
    "OFICIALIA MAYOR DE GOBIERNO": "OMG",
    "SECRETARÍA DE FINANZAS": "SF",
    "SECRETARÍA DE DESARROLLO ECONOMICO": "SEDECO",
    "SECRETARÍA DE TURISMO": "SECTUR",
    "SECRETARÍA DE INFRAESTRUCTURA": "SI",
    "SECRETARÍA DE EDUCACIÓN PÚBLICA": "SEPE",
    "SECRETARÍA DE MOVILIDAD Y TRANSPORTE": "SM",
    "COORDINACIÓN DE RADIO, CINE Y TELEVISIÓN": "CORACYT",
    "EL COLEGIO DE TLAXCALA, A.C.": "COLTLAX",
    "FIDEICOMISO DE LA CIUDAD INDUSTRIAL DE XICOTÉNCATL": "FIDECIX",
    "COMISIÓN EJECUTIVA DE ATENCIÓN A VÍCTIMAS DEL ESTADO DE TLAXCALA": "CEAVIT",
    "FONDO MACRO PARA EL DESARROLLO INTEGRAL DE TLAXCALA": "FOMTLAX",
    "INSTITUTO DE CAPACITACIÓN PARA EL TRABAJO DEL ESTADO DE TLAXCALA": "ICATLAX",
    "INSTITUTO DE CATASTRO DEL ESTADO DE TLAXCALA": "IDC",
    "INSTITUTO DEL DEPORTE DE TLAXCALA": "IDET",
    "INSTITUTO TECNOLÓGICO SUPERIOR DE TLAXCO": "ITST",
    "INSTITUTO TLAXCALTECA DE LA INFRAESTRUCTURA FÍSICA EDUCATIVA": "ITIFE",
    "PODER LEGISLATIVO DEL ESTADO DE TLAXCALA": "LEGISLATIVO",
    "INSTITUTO TLAXCALTECA DE LA JUVENTUD": "ITJ",
    "INSTITUTO TLAXCALTECA PARA LA EDUCACIÓN DE LOS ADULTOS": "ITEA",
    "INSTITUTO TLAXCALTECA PARA LA EDUCACIÓN DE LOS ADULTOS, ITEA": "ITEA",
    "ÓRGANISMO PÚBLICO DESCENTRALIZADO SALUD DE TLAXCALA": "OPD_SALUD",
    "PATRONATO CENTRO DE REHABILITACIÓN INTEGRAL Y ESCUELA EN TERAPIA FÍSICA Y REHABILITACIÓN": "CRI-ESCUELA",
    "PATRONATO \"LA LIBERTAD CENTRO CULTURAL DE APIZACO\"": "LA_LIBERTAD",
    'PATRONATO "LA LIBERTAD CENTRO CULTURAL DE APIZACO"': "LA_LIBERTAD",
    "PENSIONES CIVILES DEL ESTADO DE TLAXCALA": "PCET",
    "SISTEMA ESTATAL PARA EL DESARROLLO INTEGRAL DE LA FAMILIA": "SEDIF",
    "UNIDAD DE SERVICIOS EDUCATIVOS DEL ESTADO DE TLAXCALA": "USET",
    "UNIVERSIDAD POLITÉCNICA DE TLAXCALA": "UPT",
    "UNIVERSIDAD POLITÉCNICA DE TLAXCALA REGIÓN PONIENTE": "UPTREP",
    "PODER JUDICIAL DEL ESTADO DE TLAXCALA": "PJET",
    "UNIVERSIDAD TECNOLÓGICA DE TLAXCALA": "UTT",
    "UNIVERSIDAD INTERCULTURAL DE TLAXCALA": "UIT",
    "ARCHIVO GENERAL E HISTORICO DEL ESTADO DE TLAXCALA": "AGHET",
    "TRIBUNAL DE JUSTICIA ADMINISTRATIVA DEL ESTADO DE TLAXCALA": "TJA",
    "UNIVERSIDAD AUTÓNOMA DE TLAXCALA": "UAT",
    "COMISIÓN ESTATAL DE DERECHOS HUMANOS": "CEDH",
    "INSTITUTO TLAXCALTECA DE ELECCIONES": "ITE",
    "INSTITUTO DE ACCESO A LA INFORMACIÓN PÚBLICA Y PROTECCIÓN DE DATOS PERSONALES DEL ESTADO DE TLAXCALA": "IAIP",
    "TRIBUNAL DE CONCILIACIÓN Y ARBITRAJE DEL ESTADO DE TLAXCALA": "TCyA",
    "TRIBUNAL ELECTORAL DE TLAXCALA": "TET",
    "CENTRO DE CONCILIACIÓN LABORAL DEL ESTADO DE TLAXCALA": "CCLET",
    "FISCALÍA GENERAL DE JUSTICIA DEL ESTADO DE TLAXCALA": "FGJET",
    "SECRETARIA EJECUTIVA DEL SISTEMA ANTICORRUPCIÓN DEL ESTADO DE TLAXCALA": "SESAET",
    "PATRONATO PARA LAS EXPOSICIONES Y FERIAS EN LA CIUDAD DE TLAXCALA": "P_FERIA",
    "COMISIÓN ESTATAL DEL AGUA Y SANEAMIENTO DEL ESTADO DE TLAXCALA": "CEAS",
    "COLEGIO DE BACHILLERES DEL ESTADO DE TLAXCALA": "COBAT",
    "COLEGIO DE EDUCACIÓN PROFESIONAL TÉCNICA DEL ESTADO DE TLAXCALA": "CONALEP",
    "COLEGIO DE ESTUDIOS CIENTÍFICOS Y TECNOLÓGICOS DEL ESTADO DE TLAXCALA": "CECYTE",
    "CONSEJO ESTATAL DE POBLACIÓN": "COESPO",
}


def actualizar_siglas(db_path: str):
    """Actualiza las siglas de los entes en la base de datos."""
    if not Path(db_path).exists():
        print(f"Error: La base de datos no existe en {db_path}")
        return False

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Obtener todos los entes
    cur.execute("SELECT id, nombre, siglas FROM entes WHERE activo=1")
    entes = cur.fetchall()

    actualizados = 0
    sin_siglas = 0
    ya_tenian = 0

    for ente in entes:
        nombre = ente["nombre"].strip()
        siglas_actual = (ente["siglas"] or "").strip()

        # Buscar las siglas en el mapeo
        siglas_nueva = SIGLAS_ENTES.get(nombre, "")

        if siglas_nueva:
            if siglas_actual != siglas_nueva:
                cur.execute(
                    "UPDATE entes SET siglas=? WHERE id=?",
                    (siglas_nueva, ente["id"])
                )
                print(f"✓ Actualizado: {nombre[:60]:<60} -> {siglas_nueva}")
                actualizados += 1
            else:
                ya_tenian += 1
        else:
            if not siglas_actual:
                print(f"⚠ Sin siglas: {nombre[:60]}")
                sin_siglas += 1

    conn.commit()
    conn.close()

    print("\n" + "="*80)
    print(f"Resumen:")
    print(f"  - Entes actualizados: {actualizados}")
    print(f"  - Entes que ya tenían siglas correctas: {ya_tenian}")
    print(f"  - Entes sin siglas en el mapeo: {sin_siglas}")
    print("="*80)

    return True


if __name__ == "__main__":
    # Determinar la ruta de la base de datos
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    db_path = project_dir / "inventarios.db"

    print(f"Actualizando siglas en: {db_path}")
    print("="*80)

    actualizar_siglas(str(db_path))
