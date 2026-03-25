# src/graph_service.py
"""
Servicio de integración con Microsoft Graph API.

Encapsula toda la lógica de autenticación OAuth 2.0 y navegación por el buzón
del usuario para descargar adjuntos CSV de los correos de convenios.

Flujo principal:
  1. get_access_token()  → obtiene Bearer token de Azure AD
  2. _find_folder()      → busca el ID de la subcarpeta de correos
  3. resolver_ventana()  → convierte el filtro de fecha a un rango UTC
  4. _get_messages()     → pagina los mensajes dentro de la ventana
  5. fetch_graph_csvs()  → descarga los CSV nuevos y retorna GraphResult[]
"""

from datetime import datetime, timezone, timedelta
from typing import Optional
import httpx

from .config import TENANT_ID, CLIENT_ID, CLIENT_SEC, USER_UPN, SUBFOLDER
from .types import DateFilter, FiltroDia, FiltroRango, GraphResult

# Asunto exacto que deben tener los correos para ser considerados candidatos.
# Cualquier variación (mayúsculas, espacios extra) es rechazada.
ASUNTO_EXACTO = "Ejecucion Reporte Convenios Resumidos"

# Horas que Chile (CLT, UTC-3) está adelantado respecto a UTC.
# Se usa para convertir fechas locales Chile a timestamps UTC.
# IMPORTANTE: No considera horario de verano (CLST, UTC-4).
OFFSET_CHILE  = 3


# ── Autenticación OAuth 2.0 ───────────────────────────────────────────────────

def get_access_token() -> str:
    """
    Obtiene un Bearer token de Azure AD usando el flujo client_credentials.

    Este flujo no requiere interacción del usuario: la aplicación se autentica
    directamente con sus propias credenciales (CLIENT_ID + CLIENT_SEC).
    El token tiene una vigencia de ~1 hora y no se cachea entre ejecuciones.

    Returns:
        str: Bearer token listo para incluir en el header Authorization.

    Raises:
        RuntimeError: Si Azure AD devuelve un error en la respuesta.
        httpx.HTTPError: Si falla la comunicación de red.
    """
    resp = httpx.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SEC,
            # Scope genérico que otorga todos los permisos configurados en la app de Azure
            "scope":         "https://graph.microsoft.com/.default",
        }
    )
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"Error obteniendo token: {data.get('error_description', data)}")
    return data["access_token"]


# ── Helpers HTTP ──────────────────────────────────────────────────────────────

def _graph_get(token: str, endpoint: str) -> dict:
    """
    Realiza un GET autenticado a la Graph API y retorna el JSON de la respuesta.
    Acepta tanto paths relativos (/users/...) como URLs completas (para paginación).

    Raises:
        RuntimeError: Si el servidor devuelve un status de error (4xx, 5xx).
    """
    # Construye la URL completa si se recibe solo el path relativo
    base = endpoint if endpoint.startswith("http") else f"https://graph.microsoft.com/v1.0{endpoint}"
    resp = httpx.get(base, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    if not resp.is_success:
        raise RuntimeError(f"Graph GET {endpoint} → {resp.status_code}: {resp.text}")
    return resp.json()


def _graph_get_bytes(token: str, endpoint: str) -> bytes:
    """
    Realiza un GET autenticado a la Graph API y retorna el contenido binario.
    Usado para descargar el contenido raw de los adjuntos CSV ($value).
    Timeout extendido a 60s para archivos potencialmente grandes.
    """
    base = endpoint if endpoint.startswith("http") else f"https://graph.microsoft.com/v1.0{endpoint}"
    resp = httpx.get(base, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    if not resp.is_success:
        raise RuntimeError(f"Graph GET bytes {endpoint} → {resp.status_code}: {resp.text}")
    return resp.content


# ── Conversión de fechas Chile ↔ UTC ──────────────────────────────────────────

def _inicio_dia(fecha: str) -> datetime:
    """
    Retorna el datetime UTC correspondiente al inicio del día en hora Chile.
    Ejemplo: '2026-03-17' → 2026-03-17T03:00:00+00:00 (medianoche CLT = 03:00 UTC)
    """
    y, m, d = map(int, fecha.split("-"))
    return datetime(y, m, d, OFFSET_CHILE, 0, 0, tzinfo=timezone.utc)


def _fin_dia(fecha: str) -> datetime:
    """
    Retorna el datetime UTC correspondiente al último milisegundo del día en hora Chile.
    Ejemplo: '2026-03-17' → 2026-03-18T02:59:59.999+00:00
    """
    y, m, d = map(int, fecha.split("-"))
    return datetime(y, m, d, OFFSET_CHILE, 0, 0, tzinfo=timezone.utc) + timedelta(days=1) - timedelta(milliseconds=1)


def resolver_ventana(filtro: Optional[DateFilter]) -> tuple[datetime, datetime]:
    """
    Convierte el filtro de fecha de entrada en un par (desde, hasta) en UTC.

    Lógica por tipo de filtro:
      - None: ventana = [medianoche de hoy en Chile, ahora UTC]
      - FiltroDia: ventana = [inicio del día, fin del día] en UTC
      - FiltroRango: ventana = [inicio del primer día, fin del último día] en UTC

    Returns:
        Tuple[datetime, datetime]: (desde_utc, hasta_utc) con timezone UTC.
    """
    if filtro is None:
        # Sin filtro: procesar los correos de hoy en hora Chile
        ahora = datetime.now(timezone.utc)
        chile = ahora - timedelta(hours=OFFSET_CHILE)  # Hora local Chile
        desde = datetime(chile.year, chile.month, chile.day, OFFSET_CHILE, 0, 0, tzinfo=timezone.utc)
        return desde, ahora
    if isinstance(filtro, FiltroDia):
        return _inicio_dia(filtro.fecha), _fin_dia(filtro.fecha)
    # FiltroRango: desde el inicio del primer día hasta el fin del último
    return _inicio_dia(filtro.desde), _fin_dia(filtro.hasta)


# ── Navegación por el buzón ───────────────────────────────────────────────────

def _find_folder(token: str) -> str:
    """
    Busca el ID interno de la subcarpeta SUBFOLDER dentro del Inbox del usuario.
    La comparación es case-insensitive.

    Returns:
        str: ID interno de la carpeta en Graph API.

    Raises:
        RuntimeError: Si la subcarpeta no existe en el Inbox.
    """
    # Lista hasta 50 subcarpetas directas del Inbox
    data = _graph_get(token, f"/users/{USER_UPN}/mailFolders/Inbox/childFolders?$top=50")
    for f in data.get("value", []):
        if f["displayName"].lower() == SUBFOLDER.lower():
            return f["id"]
    raise RuntimeError(f'Subcarpeta "{SUBFOLDER}" no encontrada en Inbox de {USER_UPN}')


def _get_messages(token: str, folder_id: str, desde: datetime, hasta: datetime) -> list[dict]:
    """
    Recupera todos los mensajes de la carpeta dentro de la ventana temporal [desde, hasta].

    Estrategia de paginación:
      - Los mensajes se recuperan ordenados por fecha DESCENDENTE (más recientes primero).
      - Se pagina hasta que se encuentra un mensaje anterior a 'desde', momento en que
        se detiene la paginación (ningún mensaje posterior puede estar en la ventana).
      - Esto evita paginar toda la carpeta cuando la ventana es reciente.

    Returns:
        list[dict]: Lista de mensajes Graph con campos id, subject, receivedDateTime, hasAttachments.
    """
    resultado = []
    url = (
        f"/users/{USER_UPN}/mailFolders/{folder_id}/messages"
        f"?$orderby=receivedDateTime desc&$top=50"
        f"&$select=id,subject,receivedDateTime,hasAttachments"
    )
    while url:
        page  = _graph_get(token, url)
        salir = False
        for msg in page.get("value", []):
            # Convierte el timestamp de Graph (ISO 8601 con Z) a datetime UTC
            fecha = datetime.fromisoformat(msg["receivedDateTime"].replace("Z", "+00:00"))
            if fecha < desde:
                # Al encontrar un mensaje anterior al inicio de la ventana, no hay
                # más mensajes válidos (orden descendente garantiza esto)
                salir = True
                break
            if fecha <= hasta:
                resultado.append(msg)
        # Si encontramos un mensaje fuera de rango, detenemos la paginación.
        # Si no, continuamos con la siguiente página si existe (@odata.nextLink).
        url = None if salir else page.get("@odata.nextLink")
    return resultado


# ── Función principal ─────────────────────────────────────────────────────────

def fetch_graph_csvs(
    filenames_procesados: set[str] | None = None,
    filtro: Optional[DateFilter] = None,
) -> list[GraphResult]:
    """
    Orquesta la descarga de archivos CSV nuevos desde el buzón de Microsoft 365.

    Proceso completo:
      1. Obtiene token OAuth 2.0
      2. Localiza la subcarpeta de convenios en el Inbox
      3. Calcula la ventana temporal UTC según el filtro
      4. Recupera mensajes dentro de la ventana
      5. Filtra candidatos: solo mensajes con adjuntos y asunto exacto
      6. Para cada candidato, lista sus adjuntos y descarga los .csv no procesados
      7. Retorna la lista de GraphResult con el CSV decodificado

    Args:
        filenames_procesados: Set de nombres de archivo ya en BD (para deduplicación).
                              Si es None, se trata como conjunto vacío.
        filtro: Filtro de fecha. None = hoy en hora Chile.

    Returns:
        list[GraphResult]: Lista de adjuntos CSV descargados y listos para procesar.
    """
    if filenames_procesados is None:
        filenames_procesados = set()

    # Paso 1-3: Autenticación, localización de carpeta y cálculo de ventana
    token     = get_access_token()
    folder_id = _find_folder(token)
    desde, hasta = resolver_ventana(filtro)

    print(f"[graph] Ventana UTC: {desde.isoformat()} → {hasta.isoformat()}")

    # Paso 4: Recuperar mensajes dentro de la ventana temporal
    mensajes   = _get_messages(token, folder_id, desde, hasta)

    # Paso 5: Filtrar candidatos — solo correos con adjuntos y asunto exacto
    candidatos = [
        m for m in mensajes
        if m.get("hasAttachments")
        and (m.get("subject") or "").strip() == ASUNTO_EXACTO
    ]

    print(f"[graph] Total en ventana: {len(mensajes)} | Candidatos: {len(candidatos)}")

    # Paso 6-7: Por cada candidato, descargar los adjuntos CSV no procesados
    resultados: list[GraphResult] = []
    for msg in candidatos:
        # Lista los adjuntos del mensaje (solo metadata, no el contenido aún)
        atts = _graph_get(
            token,
            f"/users/{USER_UPN}/mailFolders/{folder_id}/messages/{msg['id']}/attachments"
            f"?$select=id,name,contentType"
        )
        for att in atts.get("value", []):
            # Saltar adjuntos que no sean CSV
            if not att["name"].lower().endswith(".csv"):
                continue
            # Saltar archivos ya procesados (deduplicación por nombre de archivo)
            if att["name"] in filenames_procesados:
                print(f"[graph] Omitiendo (ya en DB): {att['name']}")
                continue
            # Descargar el contenido binario del adjunto usando el endpoint $value
            content = _graph_get_bytes(
                token,
                f"/users/{USER_UPN}/mailFolders/{folder_id}/messages/{msg['id']}/attachments/{att['id']}/$value"
            )
            print(f"[graph] Descargado: {att['name']} ({len(content)} bytes)")
            resultados.append(GraphResult(
                message_id=  msg["id"],
                subject=     msg["subject"],
                received=    msg["receivedDateTime"],  # ISO 8601 UTC original de Graph
                filename=    att["name"],
                csv_content= content.decode("utf-8", errors="replace"),  # Decodifica a texto
            ))
    return resultados