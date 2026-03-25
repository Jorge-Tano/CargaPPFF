# src/config.py
"""
Módulo de configuración centralizada.

Lee todas las variables de entorno necesarias para el sistema usando python-dotenv.
La función _required() actúa como guardia al arranque: si alguna variable crítica
falta, lanza RuntimeError inmediatamente en lugar de fallar silenciosamente más tarde.
"""

import os
from dotenv import load_dotenv

# Carga las variables definidas en el archivo .env al entorno del proceso.
# En producción (donde .env no existe), las variables vienen del entorno del SO.
load_dotenv()


def _required(key: str) -> str:
    """
    Lee una variable de entorno y lanza RuntimeError si no está definida o está vacía.
    Garantiza que el sistema no arranque con configuración incompleta.
    """
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Variable de entorno requerida: {key}")
    return val


# ── Microsoft Graph API ───────────────────────────────────────────────────────
# Credenciales de la aplicación registrada en Azure Active Directory.
# Se usan para el flujo OAuth 2.0 client_credentials (sin interacción de usuario).

TENANT_ID  = _required("TENANT_ID")   # ID del tenant de Azure AD
CLIENT_ID  = _required("CLIENT_ID")   # Application (client) ID de la app registrada
CLIENT_SEC = _required("CLIENT_SEC")  # Client secret de la app registrada

# UPN (User Principal Name) del buzón a consultar, p.ej. "reportes@empresa.com"
USER_UPN   = _required("USER_UPN")

# Nombre de la subcarpeta dentro de Inbox donde llegan los correos de convenios.
# Por defecto "Convenios" si no se define la variable.
SUBFOLDER  = os.getenv("SUBFOLDER", "Convenios")


# ── SQL Server ────────────────────────────────────────────────────────────────
# Connection string ODBC para pyodbc.
# Usa autenticación nativa de SQL Server (UID/PWD), compatible con cualquier
# entorno sin necesidad de una cuenta de dominio Windows.
# TrustServerCertificate=yes acepta certificados auto-firmados (útil en entornos internos).

MSSQL_CONN_STR = (
    f"DRIVER={{{os.getenv('MSSQL_DRIVER', 'ODBC Driver 17 for SQL Server')}}};"
    f"SERVER={_required('MSSQL_SERVER')};"
    f"DATABASE={os.getenv('MSSQL_DATABASE', 'Informes')};"
    f"UID={_required('MSSQL_USER')};"
    f"PWD={_required('MSSQL_PASS')};"
    "TrustServerCertificate=yes;"
)


# ── PostgreSQL (base de datos destino compartida) ─────────────────────────────
# Ambos servicios (Convenios y PPFF) escriben en la misma instancia PostgreSQL.
# Los parámetros se guardan en un dict para poder construir el DSN dinámicamente.

PG_CONFIG = {
    "host":     _required("PG_HOST"),
    "port":     int(os.getenv("PG_PORT", 5432)),   # Puerto estándar por defecto
    "dbname":   _required("PG_DATABASE"),
    "user":     _required("PG_USER"),
    "password": _required("PG_PASSWORD"),
}


def pg_connstr() -> str:
    """
    Construye y retorna el DSN (Data Source Name) en el formato que espera psycopg3.
    Ejemplo de salida: "host=localhost port=5432 dbname=midb user=admin password=***"
    Se llama en cada apertura de conexión (no se cachea el string).
    """
    c = PG_CONFIG
    return (
        f"host={c['host']} port={c['port']} "
        f"dbname={c['dbname']} user={c['user']} password={c['password']}"
    )