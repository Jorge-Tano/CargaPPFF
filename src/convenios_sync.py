# src/convenios_sync.py
"""
Orquestador del servicio de sincronización de Convenios.

Responsabilidades:
  1. Invocar graph_service para obtener los CSV nuevos del buzón
  2. Parsear el contenido CSV y aplicar filtros de negocio
  3. Gestionar el versionado por día de los reportes
  4. Persistir los datos en PostgreSQL de forma atómica (una transacción por archivo)
  5. Calcular métricas de seguros por producto consultando SQL Server
  6. Retornar un resumen de la ejecución

Tablas PostgreSQL afectadas:
  - control_reportes:      metadata de cada archivo procesado
  - convenios_procesados:  registros individuales del CSV con métricas de seguro

Métricas por producto (Pago_Liviano / NORMAL / Refi_Comercial):
  - q_seg:        COUNT(desempleo>0) + COUNT(desgravamen>0) + COUNT(incapacidad>0)
                  desde Tmp_Inf_VentasWalmart en SQL Server para la fecha del archivo
  - suma_seguros: SUM(desempleo + desgravamen + incapacidad) desde SQL Server
  - conv_seg:     (q_seg / total_filas_producto_en_csv) * 100
  - por_seg:      (suma_seguros / suma_deuda_original_producto_en_csv) * 100
                  Retorna None si la deuda original total es 0.

  Todas pueden quedar en NULL si SQL Server no responde o no hay filas del producto.
  Pueden superar el 100%: un cliente puede tener más de un seguro activo.
"""

from datetime import timedelta
from typing import Optional
import psycopg

from .config import pg_connstr
from .graph_service import fetch_graph_csvs
from .mssql_service import fetch_metricas_producto
from .types import DateFilter, FiltroDia, FiltroRango, ConveniosSyncResult, ProcesadoItem

PRODUCTOS_VALIDOS    = {"Pago_Liviano", "NORMAL", "Refi_Comercial"}
PRODUCTO_PAGO_LIVIANO = "Pago_Liviano"
OFFSET_CHILE         = 3


# ── Utilidades de parseo ──────────────────────────────────────────────────────

def _parse_csv(content: str) -> list[dict]:
    lines = [l for l in content.replace("\r", "").split("\n") if l.strip()]
    if len(lines) < 2:
        return []
    headers = [h.strip().strip('"') for h in lines[0].split(";")]
    return [
        dict(zip(headers, [v.strip().strip('"') for v in line.split(";")]))
        for line in lines[1:]
    ]


def _to_num(v: str) -> float:
    try:
        return float(v.replace(".", "").replace(",", "."))
    except (ValueError, AttributeError):
        return 0.0


def _fecha_chile(iso: str) -> str:
    from datetime import datetime, timezone
    utc   = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    chile = utc - timedelta(hours=OFFSET_CHILE)
    return chile.strftime("%Y-%m-%d")


def describir_filtro(filtro: Optional[DateFilter]) -> str:
    if filtro is None:
        return "hoy (hora Chile)"
    if isinstance(filtro, FiltroDia):
        return f"día {filtro.fecha}"
    return f"rango {filtro.desde} → {filtro.hasta}"


# ── Cálculo de métricas de seguros ───────────────────────────────────────────

def _calcular_conv_seg(q_seg: int, total_producto: int) -> Optional[float]:
    """
    ConvSeg = (q_seg / total_filas_producto) * 100.
    Retorna None si no hay filas del producto (evita división por cero).
    """
    if total_producto == 0:
        return None
    return round((q_seg / total_producto) * 100, 2)


def _calcular_por_seg(suma_seguros: float, suma_deuda_original: float) -> Optional[float]:
    """
    PorSeg = (suma_seguros / suma_deuda_original) * 100.
    Retorna None si la deuda original total es 0.
    """
    if suma_deuda_original == 0:
        return None
    return round((suma_seguros / suma_deuda_original) * 100, 2)


# ── Métricas agregadas por producto ──────────────────────────────────────────

class MetricasProducto:
    """Contenedor de métricas calculadas para un producto en un archivo."""
    __slots__ = ("q_seg", "suma_seguros", "conv_seg", "por_seg")

    def __init__(
        self,
        q_seg:        Optional[int],
        suma_seguros: Optional[float],
        conv_seg:     Optional[float],
        por_seg:      Optional[float],
    ):
        self.q_seg        = q_seg
        self.suma_seguros = suma_seguros
        self.conv_seg     = conv_seg
        self.por_seg      = por_seg


def _fetch_metricas_por_producto(
    fecha_proceso: str,
    rows: list[dict],
) -> dict[str, MetricasProducto]:
    """
    Calcula métricas de seguro para cada producto presente en el CSV.

    Itera los tres productos válidos. Para cada uno:
      1. Cuenta las filas del CSV (denominador de conv_seg)
      2. Suma la deuda original del CSV (denominador de por_seg)
      3. Consulta SQL Server para q_seg y suma_seguros
      4. Calcula conv_seg y por_seg localmente

    Si un producto no tiene filas en el CSV, retorna métricas None (→ NULL en BD).
    Si SQL Server falla para un producto, ese producto queda en NULL pero los
    demás se procesan normalmente.

    Args:
        fecha_proceso: Fecha Chile 'YYYY-MM-DD' del correo.
        rows:          Filas del CSV que pasaron el filtro de negocio.

    Returns:
        Dict {nom_producto: MetricasProducto}
    """
    resultado: dict[str, MetricasProducto] = {}

    for producto in PRODUCTOS_VALIDOS:

        # Filas del CSV para este producto (denominador de conv_seg)
        filas_producto = [
            r for r in rows
            if r.get("fld_nom_producto") == producto
        ]
        total_filas = len(filas_producto)

        if total_filas == 0:
            print(
                f"[convenios] Sin filas {producto} en {fecha_proceso} "
                f"— métricas → NULL"
            )
            resultado[producto] = MetricasProducto(None, None, None, None)
            continue

        # Suma de deuda original del CSV para este producto (denominador de por_seg)
        suma_deuda = sum(
            _to_num(r.get("Deuda_Original", "0"))
            for r in filas_producto
        )

        try:
            q_seg, suma_seguros = fetch_metricas_producto(fecha_proceso, producto)
            conv_seg = _calcular_conv_seg(q_seg, total_filas)
            por_seg  = _calcular_por_seg(suma_seguros, suma_deuda)

            print(
                f"[convenios] Métricas {producto} {fecha_proceso}: "
                f"Q_Seg={q_seg} | ConvSeg={conv_seg}% | "
                f"SumaSeguros={suma_seguros} | PorSeg={por_seg}% "
                f"(base: {total_filas} filas, deuda={suma_deuda})"
            )
            resultado[producto] = MetricasProducto(q_seg, suma_seguros, conv_seg, por_seg)

        except Exception as exc:
            print(
                f"[convenios] ADVERTENCIA — No se pudo calcular métricas "
                f"para {producto} en {fecha_proceso}: {exc}"
            )
            resultado[producto] = MetricasProducto(None, None, None, None)

    return resultado


# ── Función principal ─────────────────────────────────────────────────────────

def sync_convenios(filtro: Optional[DateFilter] = None) -> ConveniosSyncResult:
    """
    Ejecuta el ciclo completo de sincronización de convenios.

    Flujo:
      1. Consulta PostgreSQL para obtener los archivos ya procesados
      2. Invoca fetch_graph_csvs() para descargar solo los CSV nuevos
      3. Por cada CSV nuevo:
         a. Determina la fecha Chile del correo
         b. Asigna la version_dia correcta
         c. Parsea el CSV y aplica filtros de negocio
         d. Calcula métricas por producto (Q_Seg, SumaSeguros, ConvSeg, PorSeg)
         e. Inserta en control_reportes y convenios_procesados en una transacción
      4. Retorna ConveniosSyncResult con el resumen

    Métricas en convenios_procesados:
      Cada fila del CSV recibe las métricas del producto al que pertenece.
      Todas las filas del mismo producto en el mismo archivo comparten
      los mismos valores de q_seg / conv_seg / por_seg / suma_seguros.
    """
    descripcion = describir_filtro(filtro)

    with psycopg.connect(pg_connstr()) as conn:

        # ── Paso 1: Deduplicación ─────────────────────────────────────────────
        with conn.cursor() as cur:
            cur.execute("SELECT archivo FROM control_reportes")
            filenames_en_db = {row[0] for row in cur.fetchall()}

        print(f"[convenios] Ya en DB: {len(filenames_en_db)} | Buscando: {descripcion}")

        # ── Paso 2: Descargar CSV nuevos ──────────────────────────────────────
        attachments = fetch_graph_csvs(filenames_en_db, filtro)
        print(f"[convenios] Nuevos CSVs: {len(attachments)}")

        if not attachments:
            return ConveniosSyncResult(
                status="no_nuevos",
                omitidos=len(filenames_en_db),
                filtro_aplicado=descripcion,
            )

        resumen: list[ProcesadoItem] = []
        total   = 0
        version_por_dia: dict[str, int] = {}

        # ── Paso 3: Procesar cada archivo ─────────────────────────────────────
        for att in reversed(attachments):

            fecha_proceso = _fecha_chile(att.received)

            if fecha_proceso not in version_por_dia:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(MAX(version_dia), 0) "
                        "FROM control_reportes WHERE fecha_proceso::date = %s::date",
                        (fecha_proceso,)
                    )
                    version_por_dia[fecha_proceso] = cur.fetchone()[0]

            version = version_por_dia[fecha_proceso] + 1
            version_por_dia[fecha_proceso] = version

            # Filtros de negocio: fld_eecc == '2call' + producto válido
            rows = [
                r for r in _parse_csv(att.csv_content)
                if r.get("fld_eecc", "").lower() == "2call"
                and r.get("fld_nom_producto", "") in PRODUCTOS_VALIDOS
            ]

            print(f"[convenios] {att.filename} ({fecha_proceso}) v{version}: {len(rows)} registros")

            # ── Métricas por producto ─────────────────────────────────────────
            # Se calculan ANTES de abrir la transacción para no retenerla durante
            # las consultas a SQL Server (una por producto = hasta 3 conexiones).
            metricas = _fetch_metricas_por_producto(fecha_proceso, rows)

            # ── Persistencia atómica ──────────────────────────────────────────
            with conn.cursor() as cur:

                # 1. Registrar el archivo en control_reportes
                #    (sin columnas de métricas — ahora viven en convenios_procesados)
                cur.execute(
                    """
                    INSERT INTO control_reportes
                        (entry_id, archivo, total_registros, version_dia,
                         fecha_proceso, email_received_at, email_subject)
                    VALUES (%s, %s, %s, %s, %s::date, %s, %s)
                    RETURNING id
                    """,
                    (att.message_id, att.filename, len(rows), version,
                     fecha_proceso, att.received, att.subject)
                )
                control_id = cur.fetchone()[0]

                # 2. Insertar registros con métricas del producto correspondiente
                if rows:
                    cur.executemany(
                        """
                        INSERT INTO convenios_procesados
                            (control_id, fld_eecc, fld_nom_producto,
                             fld_fec_con, deuda_original, fld_mto_con,
                             q_seg, conv_seg, por_seg, suma_seguros)
                        VALUES (%s, %s, %s, %s::timestamptz, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                control_id,
                                r.get("fld_eecc"),
                                r.get("fld_nom_producto"),
                                r.get("fld_fec_con") or None,
                                _to_num(r.get("Deuda_Original", "0")),
                                _to_num(r.get("fld_mto_con", "0")),
                                # Métricas del producto al que pertenece esta fila
                                metricas[r["fld_nom_producto"]].q_seg,
                                metricas[r["fld_nom_producto"]].conv_seg,
                                metricas[r["fld_nom_producto"]].por_seg,
                                metricas[r["fld_nom_producto"]].suma_seguros,
                            )
                            for r in rows
                        ]
                    )

            conn.commit()

            resumen.append(ProcesadoItem(att.filename, version, fecha_proceso, len(rows)))
            total += len(rows)

    return ConveniosSyncResult(
        status="ok",
        procesados=resumen,
        total_registros=total,
        filtro_aplicado=descripcion,
    )