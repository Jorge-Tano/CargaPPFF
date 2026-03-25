"""
main.py — Punto de entrada unificado para los servicios de sincronización.

Implementa una CLI con argparse que permite ejecutar uno o ambos servicios
de sincronización, con soporte para filtros de fecha.

Servicios disponibles:
  convenios   → Microsoft Graph → PostgreSQL (convenios_procesados)
  ppff        → SQL Server → PostgreSQL (resultado_sar)
  all         → ambos servicios en secuencia

Uso:
  python main.py convenios                                  # hoy
  python main.py convenios --fecha 2026-03-17               # día específico
  python main.py convenios --desde 2026-03-01 --hasta 2026-03-17  # rango

  python main.py ppff                                       # hoy
  python main.py ppff --fecha 2026-03-17                    # día específico

  python main.py all                                        # ambos, hoy
  python main.py all --fecha 2026-03-17                     # ambos, día específico
"""

import argparse
import logging
import sys
from datetime import date

log = logging.getLogger(__name__)


def _setup_logging():
    """
    Configura el logging con formato timestamp + nivel + mensaje.
    Nivel INFO por defecto para ver el progreso sin debug verboso.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _parse_args():
    """
    Define y parsea los argumentos de línea de comandos.

    Argumentos:
      servicio  (posicional): 'convenios', 'ppff' o 'all'
      --fecha   (opcional):  Día específico YYYY-MM-DD
      --desde   (opcional):  Inicio de rango (solo convenios)
      --hasta   (opcional):  Fin de rango (solo convenios)

    Returns:
        argparse.Namespace con los valores parseados.
    """
    parser = argparse.ArgumentParser(
        description="Sync services — Convenios (Graph) y PPFF (SQL Server)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "servicio",
        choices=["convenios", "ppff", "all"],
        help="Servicio a ejecutar",
    )
    parser.add_argument("--fecha",  help="Día específico YYYY-MM-DD")
    parser.add_argument("--desde",  help="Inicio de rango YYYY-MM-DD (solo convenios)")
    parser.add_argument("--hasta",  help="Fin de rango YYYY-MM-DD (solo convenios)")
    return parser.parse_args()


# ── Runner Servicio Convenios ─────────────────────────────────────────────────

def _run_convenios(args) -> bool:
    """
    Prepara el filtro de fecha y ejecuta el servicio de Convenios.

    Lógica de construcción del filtro:
      - --fecha solo:        FiltroDia para ese día
      - --desde + --hasta:   FiltroRango para ese período
      - --desde sin --hasta: Error (ambos son obligatorios juntos)
      - Sin argumentos:      None (= hoy en hora Chile)

    Returns:
        bool: True si la ejecución fue exitosa, False si hubo un error de argumentos.
    """
    from src.convenios_sync import sync_convenios
    from src.types import FiltroDia, FiltroRango

    # Construir el filtro según los argumentos proporcionados
    if args.fecha:
        filtro = FiltroDia(tipo="dia", fecha=args.fecha)
    elif args.desde and args.hasta:
        # Validar que el rango sea coherente
        if args.desde > args.hasta:
            log.error("--desde no puede ser posterior a --hasta")
            return False
        filtro = FiltroRango(tipo="rango", desde=args.desde, hasta=args.hasta)
    elif args.desde or args.hasta:
        # Solo uno de los dos: error de uso
        log.error("--desde y --hasta deben usarse juntos")
        return False
    else:
        filtro = None  # Sin filtro = hoy en hora Chile

    log.info("── Iniciando sync Convenios ──────────────────────────")
    result = sync_convenios(filtro)

    # Log del resultado según el status retornado
    if result.status == "no_nuevos":
        log.info(f"Sin archivos nuevos para: {result.filtro_aplicado}")
        if result.omitidos:
            log.info(f"  {result.omitidos} archivo(s) ya procesados anteriormente")
    else:
        log.info(f"Convenios completado — {len(result.procesados)} archivo(s):")
        for p in result.procesados:
            log.info(f"  {p.filename} ({p.fecha_proceso}) v{p.version_dia} → {p.registros} registros")
        log.info(f"  Total registros: {result.total_registros}")

    return True


# ── Runner Servicio PPFF ──────────────────────────────────────────────────────

def _run_ppff(args) -> bool:
    """
    Prepara la fecha y ejecuta el servicio PPFF.

    Notas:
      - Los argumentos --desde/--hasta se ignoran silenciosamente (no aplican a PPFF)
      - --fecha determina el día a procesar; sin él, se usa date.today()

    Returns:
        bool: True siempre (los errores se propagan como excepciones).
    """
    from src.ppff_sync import sync_ppff

    # Advertir si el usuario pasó --desde/--hasta (no tienen efecto en PPFF)
    if args.desde or args.hasta:
        log.warning("--desde/--hasta no aplica para ppff, se ignoran")

    # Fecha a procesar: --fecha o hoy
    fecha = date.fromisoformat(args.fecha) if args.fecha else date.today()

    log.info("── Iniciando sync PPFF ───────────────────────────────")
    result = sync_ppff(fecha)

    # Log del resultado según el status retornado
    if result.status == "sin_datos":
        log.info(f"Sin datos en Tmp_Inf_VentasWalmart para {result.fecha}")
    else:
        log.info(f"PPFF completado — sync_id={result.sync_id}  filas={result.total_filas}")

    return True


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    """
    Función principal que orquesta la ejecución de los servicios.

    Comportamiento del modo 'all':
      - Ejecuta convenios primero, luego ppff
      - Un fallo en convenios NO cancela la ejecución de ppff (son independientes)
      - El código de salida final refleja si alguno de los dos falló

    Manejo de errores:
      - Excepciones no capturadas → log.error + sys.exit(1)
      - Errores de argumentos (_run_* retorna False) → sys.exit(1) al final
    """
    _setup_logging()
    args = _parse_args()

    log.info(f"=== Sync services iniciado — servicio: {args.servicio} ===")

    ok = True
    try:
        if args.servicio == "convenios":
            ok = _run_convenios(args)

        elif args.servicio == "ppff":
            ok = _run_ppff(args)

        elif args.servicio == "all":
            # Ejecutar ambos servicios independientemente
            # (el resultado de uno no afecta la ejecución del otro)
            ok_conv = _run_convenios(args)
            ok_ppff = _run_ppff(args)
            ok = ok_conv and ok_ppff  # Falla si alguno falló

    except Exception as e:
        # Error fatal no anticipado: loguear con traceback completo y salir con código 1
        log.error(f"Error fatal: {e}", exc_info=True)
        sys.exit(1)

    log.info("=== Sync services finalizado ===")

    # Salir con código 1 si algún runner reportó fallo de argumentos
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()