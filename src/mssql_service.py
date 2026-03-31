# src/mssql_service.py
"""
Servicio de consulta a SQL Server para métricas de seguros.

Responsabilidad única: calcular métricas de seguro (Q_Seg, SumaSeguros)
para una fecha y producto dados, consultando Tmp_Inf_VentasWalmart.

Métricas calculadas:
  - q_seg:        COUNT(desempleo>0) + COUNT(desgravamen>0) + COUNT(incapacidad>0)
  - suma_seguros: SUM(desempleo + desgravamen + incapacidad)

Mapeo CSV → SQL Server (Producto / Tipo_Producto):
  - Pago_Liviano   → Producto IN ('PL','REFINANCIAMIENTO','OPCIONES DE PAGO')
                     AND Tipo_Producto = 'Pago Liviano'
  - NORMAL         → Producto IN ('REFINANCIAMIENTO','PL','OPCIONES DE PAGO')
                     AND Tipo_Producto = 'Refinanciamiento Normal'
  - Refi_Comercial → Producto = 'Refi_Comercial'
                     (sin filtro Tipo_Producto, igual que el script SQL original)
"""

import pyodbc
from .config import MSSQL_CONN_STR


# ── Mapeo fld_nom_producto (CSV) → filtros SQL Server ─────────────────────────
#
# Cada entrada define:
#   productos:      tuple para el IN clause de la columna Producto
#   tipo_producto:  valor para Tipo_Producto, o None si no aplica filtro
#
# Fuente: script ResultadoPL.sql original (documento de contexto)

_PRODUCTO_FILTROS: dict[str, dict] = {
    "Pago_Liviano": {
        "productos":     ("PL", "REFINANCIAMIENTO", "OPCIONES DE PAGO"),
        "tipo_producto": "Pago Liviano",
    },
    "NORMAL": {
        "productos":     ("REFINANCIAMIENTO", "PL", "OPCIONES DE PAGO"),
        "tipo_producto": "Refinanciamiento Normal",
    },
    "Refi_Comercial": {
        "productos":     ("Refi_Comercial",),
        "tipo_producto": None,   # Sin filtro Tipo_Producto para este producto
    },
}

# Columnas de seguro — se usan en Q_Seg (COUNT) y SumaSeguros (SUM)
_COLUMNAS_SEGURO = (
    "MontoSeguroDesempleo",
    "MontoSeguroDesgravamen",
    "MontoSeguroIncapacidadTemp",
)

# Query para COUNT de una columna de seguro específica
_QUERY_COUNT_SEGURO = """
    SELECT COUNT(*)
    FROM Informes..Tmp_Inf_VentasWalmart
    WHERE Producto IN ({placeholders})
      {filtro_tipo}
      AND CAST(Fecha_ts AS DATE) = CAST(? AS DATE)
      AND CAST({col_seguro} AS FLOAT) > 0
"""

# Query para SUM de todos los seguros en una sola pasada
_QUERY_SUMA_SEGUROS = """
    SELECT
        ISNULL(SUM(CAST(MontoSeguroDesempleo      AS FLOAT)), 0)
      + ISNULL(SUM(CAST(MontoSeguroDesgravamen    AS FLOAT)), 0)
      + ISNULL(SUM(CAST(MontoSeguroIncapacidadTemp AS FLOAT)), 0)
    FROM Informes..Tmp_Inf_VentasWalmart
    WHERE Producto IN ({placeholders})
      {filtro_tipo}
      AND CAST(Fecha_ts AS DATE) = CAST(? AS DATE)
"""


def _build_filtros(nom_producto: str) -> tuple[str, tuple, str]:
    """
    Construye los componentes SQL para el producto dado.

    Returns:
        (placeholders_in, params_productos, filtro_tipo_sql)
        - placeholders_in:    string "?, ?, ?" para el IN clause
        - params_productos:   tuple con los valores del IN
        - filtro_tipo_sql:    cláusula AND Tipo_Producto = '...' o string vacío
    """
    cfg = _PRODUCTO_FILTROS.get(nom_producto)
    if cfg is None:
        raise ValueError(f"Producto no reconocido para métricas SQL Server: {nom_producto!r}")

    placeholders = ", ".join("?" * len(cfg["productos"]))
    filtro_tipo  = (
        f"AND Tipo_Producto = '{cfg['tipo_producto']}'"
        if cfg["tipo_producto"] is not None
        else ""
    )
    return placeholders, cfg["productos"], filtro_tipo


def fetch_metricas_producto(fecha_proceso: str, nom_producto: str) -> tuple[int, float]:
    """
    Consulta SQL Server y retorna (q_seg, suma_seguros) para un producto y fecha.

    Args:
        fecha_proceso: Fecha en formato 'YYYY-MM-DD' (hora Chile del correo).
        nom_producto:  Valor de fld_nom_producto del CSV.
                       Debe ser uno de: 'Pago_Liviano', 'NORMAL', 'Refi_Comercial'.

    Returns:
        Tuple (q_seg, suma_seguros):
          - q_seg:        int   — suma de los tres COUNT de coberturas activas
          - suma_seguros: float — suma total en pesos de los tres seguros

    Raises:
        ValueError:    Si nom_producto no está en el mapeo definido.
        pyodbc.Error:  Si falla la conexión o la consulta a SQL Server.
    """
    placeholders, params_productos, filtro_tipo = _build_filtros(nom_producto)

    q_seg_total   = 0
    suma_seguros  = 0.0

    with pyodbc.connect(MSSQL_CONN_STR) as conn:
        with conn.cursor() as cur:

            # ── Q_Seg: tres COUNT independientes (uno por columna de seguro) ──
            for col in _COLUMNAS_SEGURO:
                query = _QUERY_COUNT_SEGURO.format(
                    placeholders=placeholders,
                    filtro_tipo=filtro_tipo,
                    col_seguro=col,
                )
                cur.execute(query, (*params_productos, fecha_proceso))
                row = cur.fetchone()
                q_seg_total += row[0] if row else 0

            # ── SumaSeguros: una sola query SUM ───────────────────────────────
            query_suma = _QUERY_SUMA_SEGUROS.format(
                placeholders=placeholders,
                filtro_tipo=filtro_tipo,
            )
            cur.execute(query_suma, (*params_productos, fecha_proceso))
            row = cur.fetchone()
            suma_seguros = float(row[0]) if row and row[0] is not None else 0.0

    return q_seg_total, suma_seguros


# ── Compatibilidad con código existente ───────────────────────────────────────

def fetch_q_seg(fecha_proceso: str) -> int:
    """
    Mantiene la interfaz anterior (solo Q_Seg para Pago_Liviano).
    Wrappea fetch_metricas_producto para no romper imports existentes.

    Deprecated: usar fetch_metricas_producto directamente.
    """
    q_seg, _ = fetch_metricas_producto(fecha_proceso, "Pago_Liviano")
    return q_seg