# src/mssql_service.py
"""
Servicio de consulta a SQL Server para métricas de seguros.

Responsabilidad única: calcular Q_Seg (cantidad de coberturas de seguro)
para una fecha dada, consultando la tabla Tmp_Inf_VentasWalmart.

Q_Seg es la suma de tres conteos independientes (uno por tipo de seguro):
  - MontoSeguroDesempleo     > 0
  - MontoSeguroDesgravamen   > 0
  - MontoSeguroIncapacidadTemp > 0

Filtros comunes a los tres conteos:
  - Producto IN ('PL', 'REFINANCIAMIENTO', 'OPCIONES DE PAGO')
  - Tipo_Producto = 'Pago Liviano'
  - CAST(Fecha_ts AS DATE) = fecha dada

Solo se expone la función fetch_q_seg(fecha_proceso) que retorna el entero Q_Seg.
ConvSeg se calcula en convenios_sync porque necesita el denominador del CSV.
"""

import pyodbc
from .config import MSSQL_CONN_STR


# Productos de SQL Server que corresponden al segmento Pago Liviano.
# Deben coincidir exactamente con los valores en Tmp_Inf_VentasWalmart.
_PRODUCTOS_PL = ("PL", "REFINANCIAMIENTO", "OPCIONES DE PAGO")

# Template de la query parametrizada para un tipo de seguro.
# Se usa tres veces (una por columna de seguro) y se suman los resultados.
_QUERY_SEGURO = """
    SELECT COUNT(*)
    FROM Informes..Tmp_Inf_VentasWalmart
    WHERE Producto IN ({placeholders})
      AND Tipo_Producto = 'Pago Liviano'
      AND CAST(Fecha_ts AS DATE) = CAST(? AS DATE)
      AND CAST({col_seguro} AS FLOAT) > 0
"""

# Columnas de seguro que se suman para obtener Q_Seg
_COLUMNAS_SEGURO = (
    "MontoSeguroDesempleo",
    "MontoSeguroDesgravamen",
    "MontoSeguroIncapacidadTemp",
)


def fetch_q_seg(fecha_proceso: str) -> int:
    """
    Consulta SQL Server y retorna Q_Seg para la fecha dada.

    Q_Seg = COUNT(desempleo > 0) + COUNT(desgravamen > 0) + COUNT(incapacidad > 0)
    para Producto IN ('PL','REFINANCIAMIENTO','OPCIONES DE PAGO')
    y Tipo_Producto = 'Pago Liviano' en la fecha indicada.

    Args:
        fecha_proceso: Fecha en formato 'YYYY-MM-DD' (hora Chile del correo).

    Returns:
        int: Suma de los tres conteos. 0 si no hay datos o falla la conexión.

    Raises:
        pyodbc.Error: Si falla la conexión o la consulta a SQL Server.
    """
    # Construye los placeholders para el IN clause según la cantidad de productos
    placeholders = ", ".join("?" * len(_PRODUCTOS_PL))

    q_seg_total = 0

    with pyodbc.connect(MSSQL_CONN_STR) as conn:
        with conn.cursor() as cur:
            for col in _COLUMNAS_SEGURO:
                query = _QUERY_SEGURO.format(
                    placeholders=placeholders,
                    col_seguro=col,
                )
                # Parámetros: primero los productos del IN, luego la fecha
                cur.execute(query, (*_PRODUCTOS_PL, fecha_proceso))
                row = cur.fetchone()
                q_seg_total += row[0] if row else 0

    return q_seg_total