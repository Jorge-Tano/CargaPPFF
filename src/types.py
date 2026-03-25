# src/types.py
"""
Modelos de datos compartidos entre módulos.

Centraliza todas las estructuras de datos del sistema como dataclasses de Python.
El uso de tipado estricto y dataclasses garantiza:
  - Contratos claros entre módulos (el compilador de tipos detecta errores antes de ejecutar)
  - Fácil serialización para logging y tests
  - Ausencia de errores por atributos mal escritos (AttributeError en runtime)
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional, Union


# ── Filtros de fecha — Servicio Convenios ─────────────────────────────────────
# Permiten acotar el rango de correos a procesar en Microsoft Graph.
# El tipo unión DateFilter se usa como parámetro en sync_convenios() y fetch_graph_csvs().

@dataclass
class FiltroDia:
    """
    Filtra correos recibidos en un único día específico (hora Chile).
    Ejemplo: FiltroDia(tipo="dia", fecha="2026-03-17")
    """
    tipo: Literal["dia"]   # Discriminador del tipo unión
    fecha: str             # Fecha en formato ISO 8601: 'YYYY-MM-DD'


@dataclass
class FiltroRango:
    """
    Filtra correos recibidos entre dos fechas, ambas inclusive (hora Chile).
    Ejemplo: FiltroRango(tipo="rango", desde="2026-03-01", hasta="2026-03-17")
    """
    tipo: Literal["rango"]  # Discriminador del tipo unión
    desde: str              # Fecha de inicio en formato 'YYYY-MM-DD'
    hasta: str              # Fecha de fin en formato 'YYYY-MM-DD'


# Tipo unión: el filtro puede ser un día, un rango, o None (= hoy en hora Chile)
DateFilter = Union[FiltroDia, FiltroRango]


# ── Tipos del Servicio Convenios ──────────────────────────────────────────────

@dataclass
class GraphResult:
    """
    Representa un adjunto CSV descargado exitosamente desde Microsoft Graph API.
    Producido por graph_service.fetch_graph_csvs() y consumido por convenios_sync.
    """
    message_id:  str   # ID interno del mensaje en Graph API (usado para deduplicación)
    subject:     str   # Asunto del correo (debe ser exactamente ASUNTO_EXACTO)
    received:    str   # Timestamp ISO 8601 UTC de recepción, ej: "2026-03-17T13:05:00Z"
    filename:    str   # Nombre del archivo adjunto CSV, ej: "reporte_20260317.csv"
    csv_content: str   # Contenido completo del CSV decodificado en UTF-8


@dataclass
class ProcesadoItem:
    """
    Resumen de un archivo CSV procesado exitosamente.
    Incluido en ConveniosSyncResult.procesados para el log de la ejecución.
    """
    filename:      str   # Nombre del archivo CSV procesado
    version_dia:   int   # Versión asignada dentro del día Chile (1 = primero del día)
    fecha_proceso: str   # Fecha Chile calculada del correo, formato 'YYYY-MM-DD'
    registros:     int   # Cantidad de filas insertadas en convenios_procesados


@dataclass
class ConveniosSyncResult:
    """
    Resultado completo de una ejecución del servicio Convenios.
    Retornado por sync_convenios() y usado por main.py para el log final.
    """
    status: Literal["ok", "no_nuevos"]          # 'ok' si se procesó al menos un archivo
    procesados: list[ProcesadoItem] = field(default_factory=list)  # Archivos procesados
    omitidos: int = 0                            # Archivos ya en DB que se saltaron
    total_registros: int = 0                     # Suma de registros de todos los archivos
    filtro_aplicado: str = ""                    # Descripción legible del filtro usado


# ── Tipos del Servicio PPFF ───────────────────────────────────────────────────

@dataclass
class VentaRow:
    """
    Representa una fila de venta leída desde SQL Server (Tmp_Inf_VentasWalmart).
    Producida por ppff_sync.leer_ventas() y usada en calcular_metricas() y guardar_sync().
    """
    fecha_ts:                  Optional[datetime]  # Timestamp de la transacción (puede ser NULL)
    producto:                  str                 # Código de producto en mayúsculas: 'AV', 'SAV', etc.
    monto:                     float               # Capital colocado (monto del crédito)
    monto_total_pagar:         float               # Total a pagar incluyendo intereses y seguros
    monto_seguro_desempleo:    float               # Prima del seguro de cesantía/desempleo
    monto_seguro_desgravamen:  float               # Prima del seguro de desgravamen (vida)
    monto_seguro_incapacidad:  float               # Prima del seguro de incapacidad temporal
    origen:                    str                 # Canal de venta en mayúsculas: 'INB' o 'OUT'
    crm:                       str                 # Código del CRM/campaña de origen
    tipo_producto:             Optional[str]       # Clasificación adicional (puede ser NULL)


@dataclass
class Segmento:
    """
    Métricas calculadas para un segmento de producto (subconjunto de VentaRow).
    Producida por ppff_sync._calcular() e insertada en la tabla resultado_sar.
    """
    nombre:              str    # Identificador del segmento: 'AV', 'SAV', 'Total', etc.
    op:                  int   = 0      # Cantidad de operaciones (filas que cumplen el filtro)
    capital:             float = 0.0   # Suma de monto (capital colocado total)
    promedio_capital:    int   = 0     # capital / op, redondeado a entero
    financiado:          float = 0.0   # Suma de monto_total_pagar
    promedio_financiado: int   = 0     # financiado / op, redondeado a entero
    conv_seguros:        float = 0.0   # % de coberturas de seguro sobre operaciones (0 para 'Total')
    suma_seguros:        float = 0.0   # Suma de los tres tipos de seguros
    por_seguro:          float = 0.0   # suma_seguros / capital * 100 (0 para 'Total')


@dataclass
class PPFFSyncResult:
    """
    Resultado completo de una ejecución del servicio PPFF.
    Retornado por sync_ppff() y usado por main.py para el log final.
    """
    status:      Literal["ok", "sin_datos"]  # 'sin_datos' si SQL Server no tiene filas para la fecha
    sync_id:     Optional[int] = None        # ID generado en sync_log_ppff (None si sin_datos)
    total_filas: int = 0                     # Total de filas leídas de SQL Server
    fecha:       str = ""                    # Fecha procesada en formato 'YYYY-MM-DD'