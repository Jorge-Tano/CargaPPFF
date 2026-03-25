# src/ppff_sync.py
"""
Orquestador del servicio de sincronización PPFF (Productos Financieros).

Responsabilidades:
  1. Leer filas de ventas desde SQL Server (Tmp_Inf_VentasWalmart)
  2. Calcular métricas por segmento de producto (AV, SAV y sub-segmentos)
  3. Persistir en PostgreSQL: datos crudos + métricas calculadas en una transacción

Tablas PostgreSQL afectadas:
  - sync_log_ppff: log de cada ejecución de sync (estado, timestamps, totales)
  - ventas_walmart: copia fiel de los datos crudos leídos de SQL Server
  - resultado_sar: métricas calculadas por segmento de producto

Nota sobre el nombre PPFF: hace referencia a la familia de Productos Financieros
gestionados por el módulo (AV = Avance de efectivo, SAV = Seguro Avance).
"""

from contextlib import contextmanager
from datetime import date
from typing import Callable
import pyodbc
import psycopg

from .config import MSSQL_CONN_STR, pg_connstr
from .types import VentaRow, Segmento, PPFFSyncResult


# ── Conexión a SQL Server ─────────────────────────────────────────────────────

@contextmanager
def _mssql():
    """
    Context manager que abre y cierra de forma segura una conexión a SQL Server.
    Usa autenticación Windows integrada (configurada en MSSQL_CONN_STR).
    Garantiza que la conexión se cierre incluso si ocurre una excepción.

    Usage:
        with _mssql() as cur:
            cur.execute("SELECT ...")
    """
    conn   = pyodbc.connect(MSSQL_CONN_STR)
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


def _safe(val) -> float:
    """
    Convierte un valor a float de forma segura.
    Retorna 0.0 para None, cadenas vacías o valores no convertibles.
    Evita errores al procesar columnas de SQL Server que pueden contener NULL.
    """
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return 0.0


# ── Extracción de datos desde SQL Server ──────────────────────────────────────

def leer_ventas(fecha: date) -> list[VentaRow]:
    """
    Lee todas las filas de ventas de una fecha específica desde SQL Server.

    Consulta la tabla Tmp_Inf_VentasWalmart filtrando por la fecha de la
    transacción (CAST(Fecha_ts AS DATE) = ?). Los strings se normalizan
    a mayúsculas para simplificar las comparaciones en calcular_metricas().

    Args:
        fecha: Fecha para la cual se quieren obtener las ventas.

    Returns:
        list[VentaRow]: Lista de ventas del día. Vacía si no hay datos.
    """
    with _mssql() as cur:
        cur.execute(
            """
            SELECT Fecha_ts, Producto, Monto, MontoTotalPagar,
                   MontoSeguroDesempleo, MontoSeguroDesgravamen,
                   MontoSeguroIncapacidadTemp, Origen, Crm, Tipo_Producto
            FROM Informes..Tmp_Inf_VentasWalmart
            WHERE CAST(Fecha_ts AS DATE) = ?
            """,
            fecha.isoformat()  # Parámetro ODBC: '2026-03-17'
        )
        cols = [c[0] for c in cur.description]
        return [
            VentaRow(
                fecha_ts=                 r[0],
                producto=                 (r[1] or "").strip().upper(),  # Normalizar a mayúsculas
                monto=                    _safe(r[2]),
                monto_total_pagar=        _safe(r[3]),
                monto_seguro_desempleo=   _safe(r[4]),
                monto_seguro_desgravamen= _safe(r[5]),
                monto_seguro_incapacidad= _safe(r[6]),
                origen=                   (r[7] or "").strip().upper(),  # Normalizar a mayúsculas
                crm=                      str(r[8] or "").strip(),
                tipo_producto=            r[9],
            )
            for r in cur.fetchall()
        ]


# ── Definición de segmentos y cálculo de métricas ────────────────────────────

# Lista de segmentos de producto con sus filtros de clasificación.
# Cada tupla contiene: (nombre_segmento, función_filtro)
# La función filtro recibe un VentaRow y retorna True si pertenece al segmento.
#
# Segmentos AV (Avance de efectivo):
#   AV         → Todas las operaciones de Avance
#   AV-INB     → Avance canal Inbound (llamadas entrantes)
#   AV-OUT     → Avance canal Outbound con CRM 116 (campaña específica)
#   AV-LEAKAGE → Avance tipo leakage (CRM 250, detección de fugas)
#
# Segmentos SAV (Seguro Avance):
#   SAV         → Toda la familia SAV
#   SAV-INB     → SAV canal Inbound
#   SAV-OUT     → SAV canal Outbound con CRMs específicos
#   SAV-LEAKAGE → SAV tipo leakage (CRM 265)
#
# Total → Suma de toda la familia AV + SAV (sin cálculo de seguros)

FILTROS: list[tuple[str, Callable[[VentaRow], bool]]] = [
    ("AV",          lambda r: r.producto in ("AV", "AVANCE")),
    ("AV-INB",      lambda r: r.producto in ("AV", "AVANCE") and r.origen == "INB"),
    ("AV-OUT",      lambda r: r.producto in ("AV", "AVANCE") and r.origen == "OUT" and r.crm == "116"),
    ("AV-LEAKAGE",  lambda r: r.producto == "AVANCE" and r.origen == "OUT" and r.crm == "250"),
    ("SAV",         lambda r: r.producto in ("SAV", "LEAKAGE SAV", "AV SAV", "SAV MONTOS BAJOS", "SAR")),
    ("SAV-INB",     lambda r: r.producto in ("SAV", "SAR") and r.origen == "INB"),
    ("SAV-OUT",     lambda r: r.producto in ("SAV", "AV SAV", "SAV MONTOS BAJOS", "SAR") and r.origen == "OUT" and r.crm in ("2", "250", "17", "116")),
    ("SAV-LEAKAGE", lambda r: r.producto == "LEAKAGE SAV" and r.origen == "OUT" and r.crm == "265"),
    ("Total",       lambda r: r.producto in ("AV", "AVANCE", "SAV", "LEAKAGE SAV", "AV SAV", "SAV MONTOS BAJOS", "SAR")),
]


def _calcular(nombre: str, filas: list[VentaRow]) -> Segmento:
    """
    Calcula las métricas de un segmento a partir de sus filas filtradas.

    Métricas calculadas:
      - op: cantidad de operaciones
      - capital / promedio_capital: monto colocado total y promedio
      - financiado / promedio_financiado: total a pagar (con intereses y seguros)
      - conv_seguros: % de coberturas de seguro sobre operaciones
                      (0 para el segmento Total, ya que agrega sub-segmentos)
      - suma_seguros: suma de los tres tipos de prima (desempleo + desgravamen + incapacidad)
      - por_seguro: proporción de seguros sobre el capital total

    Args:
        nombre: Nombre del segmento (ej: 'AV-INB')
        filas:  Filas de VentaRow que pertenecen a este segmento

    Returns:
        Segmento con todas las métricas calculadas. Si filas está vacío, retorna Segmento con ceros.
    """
    if not filas:
        return Segmento(nombre=nombre)  # Segmento vacío, todos los valores en 0

    op         = len(filas)
    capital    = sum(r.monto for r in filas)
    financiado = sum(r.monto_total_pagar for r in filas)

    # Suma de primas de los tres tipos de seguro para todas las operaciones
    suma_seg = sum(
        r.monto_seguro_desempleo + r.monto_seguro_desgravamen + r.monto_seguro_incapacidad
        for r in filas
    )

    # Cuenta coberturas individuales (una operación puede tener 1, 2 o 3 coberturas)
    n_seg = sum(
        (1 if r.monto_seguro_desempleo    > 0 else 0) +
        (1 if r.monto_seguro_desgravamen  > 0 else 0) +
        (1 if r.monto_seguro_incapacidad  > 0 else 0)
        for r in filas
    )

    # Para el segmento "Total" no se calculan métricas de seguros
    # (evita doble conteo ya que Total incluye todos los demás segmentos)
    conv_seg = 0.0 if nombre == "Total" else round(n_seg * 100.0 / op, 2)
    por_seg  = 0.0 if nombre == "Total" else (round(suma_seg * 100.0 / capital, 2) if capital > 0 else 0.0)

    return Segmento(
        nombre=nombre,
        op=op,
        capital=round(capital, 2),
        promedio_capital=int(capital / op) if op > 0 else 0,
        financiado=round(financiado, 2),
        promedio_financiado=int(financiado / op) if op > 0 else 0,
        conv_seguros=conv_seg,
        suma_seguros=round(suma_seg, 2),
        por_seguro=por_seg,
    )


def calcular_metricas(filas: list[VentaRow]) -> list[Segmento]:
    """
    Aplica todos los filtros de FILTROS y calcula las métricas para cada segmento.

    Para cada (nombre, filtro) en FILTROS:
      1. Filtra las filas que cumplen el criterio del segmento
      2. Calcula las métricas del grupo filtrado
      3. Retorna el Segmento resultante

    Returns:
        list[Segmento]: Lista de 9 segmentos con sus métricas (en el orden de FILTROS).
    """
    return [_calcular(nombre, [r for r in filas if f(r)]) for nombre, f in FILTROS]


# ── Persistencia en PostgreSQL ────────────────────────────────────────────────

def guardar_sync(fecha: date, filas: list[VentaRow], segmentos: list[Segmento]) -> int:
    """
    Persiste todos los datos del sync en PostgreSQL en una única transacción.

    Secuencia de operaciones (todas atómicas):
      1. INSERT en sync_log_ppff con estado 'iniciado' → obtiene sync_id
      2. INSERT masivo de todas las filas crudas en ventas_walmart
      3. INSERT masivo de los 9 segmentos en resultado_sar
      4. UPDATE sync_log_ppff: estado 'completado', finalizado_en = NOW(), total_filas
      5. COMMIT: si cualquier paso falla, se hace rollback de todo

    Args:
        fecha:     Fecha de los datos procesados.
        filas:     Lista de VentaRow leídas de SQL Server.
        segmentos: Lista de Segmento calculados por calcular_metricas().

    Returns:
        int: ID del registro creado en sync_log_ppff (sync_id).
    """
    with psycopg.connect(pg_connstr()) as conn:
        with conn.cursor() as cur:

            # Paso 1: Registrar inicio del sync y obtener el sync_id
            cur.execute(
                "INSERT INTO sync_log_ppff (fecha_datos, estado) VALUES (%s, 'iniciado') RETURNING id",
                (fecha,)
            )
            sync_id = cur.fetchone()[0]

            # Paso 2: Insertar todas las filas crudas de SQL Server
            # Vinculadas al sync_id para trazabilidad
            cur.executemany(
                """
                INSERT INTO ventas_walmart (
                    sync_id, fecha_ts, producto, monto, monto_total_pagar,
                    monto_seguro_desempleo, monto_seguro_desgravamen, monto_seguro_incapacidad,
                    origen, crm, tipo_producto
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [(sync_id, f.fecha_ts, f.producto, f.monto, f.monto_total_pagar,
                  f.monto_seguro_desempleo, f.monto_seguro_desgravamen, f.monto_seguro_incapacidad,
                  f.origen, f.crm, f.tipo_producto) for f in filas]
            )

            # Paso 3: Insertar las métricas calculadas por segmento
            cur.executemany(
                """
                INSERT INTO resultado_sar (
                    sync_id, fecha_datos, nombre_producto, op, capital, promedio_capital,
                    financiado, promedio_financiado, conv_seguros, suma_seguros, por_seguro
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [(sync_id, fecha, s.nombre, s.op, s.capital, s.promedio_capital,
                  s.financiado, s.promedio_financiado, s.conv_seguros, s.suma_seguros, s.por_seguro)
                 for s in segmentos]
            )

            # Paso 4: Marcar el sync como completado con su timestamp final
            cur.execute(
                "UPDATE sync_log_ppff SET finalizado_en = NOW(), total_filas = %s, estado = 'completado' WHERE id = %s",
                (len(filas), sync_id)
            )

        # Paso 5: Confirmar toda la transacción
        conn.commit()
    return sync_id


# ── Orquestador principal ─────────────────────────────────────────────────────

def sync_ppff(fecha: date) -> PPFFSyncResult:
    """
    Ejecuta el ciclo completo de sincronización PPFF para una fecha dada.

    Flujo:
      1. Lee las ventas de la fecha desde SQL Server
      2. Si no hay datos, retorna con status 'sin_datos'
      3. Calcula métricas por segmento
      4. Persiste datos crudos y métricas en PostgreSQL
      5. Registra los resultados en consola y retorna PPFFSyncResult

    Args:
        fecha: Fecha para la cual procesar las ventas (normalmente date.today()).

    Returns:
        PPFFSyncResult con status 'ok' o 'sin_datos'.
    """
    print(f"[ppff] Leyendo Tmp_Inf_VentasWalmart para {fecha}...")
    filas = leer_ventas(fecha)
    print(f"[ppff] {len(filas)} filas leídas")

    # Sin datos para la fecha: retornar sin intentar persistir
    if not filas:
        return PPFFSyncResult(status="sin_datos", fecha=fecha.isoformat())

    # Calcular métricas y persistir en una única transacción
    segmentos = calcular_metricas(filas)
    sync_id   = guardar_sync(fecha, filas, segmentos)

    # Log de los resultados por segmento para trazabilidad
    for s in segmentos:
        print(f"[ppff]   {s.nombre:<14} op={s.op:>4}  capital={s.capital:>14,.0f}  conv_seg={s.conv_seguros:.2f}%")

    return PPFFSyncResult(
        status="ok",
        sync_id=sync_id,
        total_filas=len(filas),
        fecha=fecha.isoformat()
    )