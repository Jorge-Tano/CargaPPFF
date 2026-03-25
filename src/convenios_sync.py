# src/convenios_sync.py
"""
Orquestador del servicio de sincronización de Convenios.

Responsabilidades:
  1. Invocar graph_service para obtener los CSV nuevos del buzón
  2. Parsear el contenido CSV y aplicar filtros de negocio
  3. Gestionar el versionado por día de los reportes
  4. Persistir los datos en PostgreSQL de forma atómica (una transacción por archivo)
  5. Retornar un resumen de la ejecución

Tablas PostgreSQL afectadas:
  - control_reportes: registro de metadata de cada archivo procesado
  - convenios_procesados: registros individuales filtrados de cada CSV
"""

from datetime import timedelta
from typing import Optional
import psycopg

from .config import pg_connstr
from .graph_service import fetch_graph_csvs
from .types import DateFilter, FiltroDia, FiltroRango, ConveniosSyncResult, ProcesadoItem

# Valores válidos para el campo fld_nom_producto.
# Solo se insertan filas cuyo producto esté en este conjunto.
PRODUCTOS_VALIDOS = {"Pago_Liviano", "NORMAL", "Refi_Comercial"}

# Diferencia horaria entre UTC y Chile (CLT = UTC-3).
# Se suma al timestamp UTC para obtener la hora local chilena.
OFFSET_CHILE = 3


# ── Utilidades de parseo ──────────────────────────────────────────────────────

def _parse_csv(content: str) -> list[dict]:
    """
    Parsea el contenido de un archivo CSV con separador punto y coma (;).
    Implementación propia sin dependencias externas para mayor control.

    Proceso:
      1. Normaliza saltos de línea Windows (\\r\\n → \\n)
      2. Descarta líneas vacías
      3. Usa la primera línea como headers (limpia comillas y espacios)
      4. Mapea cada línea posterior a un diccionario {header: valor}

    Returns:
        list[dict]: Lista de filas como diccionarios. Vacía si el CSV tiene menos de 2 líneas.
    """
    lines = [l for l in content.replace("\r", "").split("\n") if l.strip()]
    if len(lines) < 2:
        return []  # CSV vacío o solo con headers
    headers = [h.strip().strip('"') for h in lines[0].split(";")]
    return [
        dict(zip(headers, [v.strip().strip('"') for v in line.split(";")]))
        for line in lines[1:]
    ]


def _to_num(v: str) -> float:
    """
    Convierte un string numérico en formato español/chileno a float.
    Maneja el formato "1.234,56" → 1234.56:
      - Elimina el separador de miles (punto)
      - Reemplaza la coma decimal por punto

    Retorna 0.0 si la conversión falla (valores vacíos, None, texto no numérico).
    """
    try:
        return float(v.replace(".", "").replace(",", "."))
    except (ValueError, AttributeError):
        return 0.0


def _fecha_chile(iso: str) -> str:
    """
    Convierte un timestamp ISO 8601 UTC a la fecha local en Chile (CLT = UTC-3).

    Ejemplo: '2026-03-18T01:30:00Z' → '2026-03-17'
    (La medianoche chilena son las 03:00 UTC, por lo que antes de las 03:00 UTC
    sigue siendo el día anterior en Chile)

    Args:
        iso: Timestamp en formato ISO 8601 con 'Z' o offset UTC.

    Returns:
        str: Fecha Chile en formato 'YYYY-MM-DD'.
    """
    from datetime import datetime, timezone
    utc   = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    chile = utc - timedelta(hours=OFFSET_CHILE)
    return chile.strftime("%Y-%m-%d")


def describir_filtro(filtro: Optional[DateFilter]) -> str:
    """
    Genera una descripción legible del filtro para logging.
    Usada en el campo filtro_aplicado del resultado y en los logs de main.py.
    """
    if filtro is None:
        return "hoy (hora Chile)"
    if isinstance(filtro, FiltroDia):
        return f"día {filtro.fecha}"
    return f"rango {filtro.desde} → {filtro.hasta}"


# ── Función principal ─────────────────────────────────────────────────────────

def sync_convenios(filtro: Optional[DateFilter] = None) -> ConveniosSyncResult:
    """
    Ejecuta el ciclo completo de sincronización de convenios.

    Flujo:
      1. Consulta PostgreSQL para obtener los archivos ya procesados (deduplicación)
      2. Invoca fetch_graph_csvs() para descargar solo los CSV nuevos
      3. Por cada CSV nuevo:
         a. Determina la fecha Chile del correo
         b. Asigna la version_dia correcta (MAX existente + 1)
         c. Parsea el CSV y aplica filtros de negocio (fld_eecc + producto)
         d. Inserta en control_reportes y convenios_procesados en una transacción
      4. Retorna un ConveniosSyncResult con el resumen de la ejecución

    Args:
        filtro: Acota el rango de fechas de búsqueda en el buzón.
                None = solo correos de hoy en hora Chile.

    Returns:
        ConveniosSyncResult con status 'ok' o 'no_nuevos'.
    """
    descripcion = describir_filtro(filtro)

    with psycopg.connect(pg_connstr()) as conn:

        # ── Paso 1: Obtener los archivos ya procesados para deduplicación ────
        with conn.cursor() as cur:
            cur.execute("SELECT archivo FROM control_reportes")
            # Set de nombres de archivo para O(1) lookup en la deduplicación
            filenames_en_db = {row[0] for row in cur.fetchall()}

        print(f"[convenios] Ya en DB: {len(filenames_en_db)} | Buscando: {descripcion}")

        # ── Paso 2: Descargar CSV nuevos desde Microsoft Graph ────────────────
        attachments = fetch_graph_csvs(filenames_en_db, filtro)
        print(f"[convenios] Nuevos CSVs: {len(attachments)}")

        # Si no hay archivos nuevos, retornar inmediatamente
        if not attachments:
            return ConveniosSyncResult(
                status="no_nuevos",
                omitidos=len(filenames_en_db),
                filtro_aplicado=descripcion,
            )

        resumen: list[ProcesadoItem] = []
        total   = 0

        # Cache de versiones por día para evitar consultas repetidas a la BD
        # cuando múltiples archivos caen en el mismo día Chile
        version_por_dia: dict[str, int] = {}

        # ── Paso 3: Procesar cada archivo CSV ────────────────────────────────
        # reversed() procesa del más antiguo al más reciente, de modo que
        # la versión más reciente del día quede con el número de versión mayor
        for att in reversed(attachments):

            # Determina a qué día Chile corresponde este correo
            fecha_proceso = _fecha_chile(att.received)

            # Calcula la version_dia para este fecha_proceso.
            # Si ya procesamos otro archivo del mismo día en este ciclo, usamos el cache.
            # Si no, consultamos la BD para ver el máximo existente.
            if fecha_proceso not in version_por_dia:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(MAX(version_dia), 0) FROM control_reportes WHERE fecha_proceso::date = %s::date",
                        (fecha_proceso,)
                    )
                    version_por_dia[fecha_proceso] = cur.fetchone()[0]

            # La siguiente versión es el máximo actual + 1
            version = version_por_dia[fecha_proceso] + 1
            version_por_dia[fecha_proceso] = version  # Actualizar cache

            # ── Filtros de negocio ────────────────────────────────────────────
            # Solo se insertan filas que cumplan AMBAS condiciones:
            #   1. fld_eecc == '2call' (case-insensitive)
            #   2. fld_nom_producto ∈ PRODUCTOS_VALIDOS
            rows = [
                r for r in _parse_csv(att.csv_content)
                if r.get("fld_eecc", "").lower() == "2call"
                and r.get("fld_nom_producto", "") in PRODUCTOS_VALIDOS
            ]

            print(f"[convenios] {att.filename} ({fecha_proceso}) v{version}: {len(rows)} registros")

            # ── Persistencia atómica (una transacción por archivo) ────────────
            with conn.cursor() as cur:

                # 1. Registrar el archivo en la tabla de control
                cur.execute(
                    """
                    INSERT INTO control_reportes
                        (entry_id, archivo, total_registros, version_dia,
                         fecha_proceso, email_received_at, email_subject)
                    VALUES (%s, %s, %s, %s, %s::date, %s, %s) RETURNING id
                    """,
                    (att.message_id, att.filename, len(rows), version,
                     fecha_proceso, att.received, att.subject)
                )
                # ID generado por la BD, usado como FK en convenios_procesados
                control_id = cur.fetchone()[0]

                # 2. Insertar los registros filtrados del CSV (puede ser 0 si ninguno pasó el filtro)
                if rows:
                    cur.executemany(
                        """
                        INSERT INTO convenios_procesados
                            (control_id, fld_eecc, fld_nom_producto,
                             fld_fec_con, deuda_original, fld_mto_con)
                        VALUES (%s, %s, %s, %s::timestamptz, %s, %s)
                        """,
                        [(control_id,
                          r.get("fld_eecc"),
                          r.get("fld_nom_producto"),
                          r.get("fld_fec_con") or None,  # None si la fecha está vacía
                          _to_num(r.get("Deuda_Original", "0")),
                          _to_num(r.get("fld_mto_con", "0"))) for r in rows]
                    )

            # Confirma la transacción para este archivo.
            # Si falla cualquier INSERT anterior, el rollback automático de psycopg
            # revierte tanto el control_reportes como los convenios_procesados.
            conn.commit()

            resumen.append(ProcesadoItem(att.filename, version, fecha_proceso, len(rows)))
            total += len(rows)

    return ConveniosSyncResult(
        status="ok",
        procesados=resumen,
        total_registros=total,
        filtro_aplicado=descripcion,
    )