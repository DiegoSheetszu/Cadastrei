from sqlalchemy import create_engine
from config.settings import settings
from urllib.parse import quote_plus


def _resolver_driver_odbc(driver_configurado: str) -> str:
    preferido = (driver_configurado or "").strip()

    try:
        import pyodbc
    except Exception as exc:
        raise RuntimeError("Modulo pyodbc indisponivel para conexao SQL Server.") from exc

    instalados = [d.strip() for d in pyodbc.drivers() if d and d.strip()]

    if preferido and preferido in instalados:
        return preferido

    fallback = (
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 11 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    )
    for driver in fallback:
        if driver in instalados:
            return driver

    if preferido:
        raise RuntimeError(
            f"Driver ODBC nao encontrado: {preferido!r}. "
            f"Drivers instalados: {', '.join(instalados) if instalados else 'nenhum'}."
        )

    raise RuntimeError(
        "Nenhum driver ODBC para SQL Server foi encontrado no sistema. "
        "Instale ODBC Driver 17/18 for SQL Server."
    )


def ativar_engine(database: str):

    driver = quote_plus(_resolver_driver_odbc(settings.db_driver))
    user = quote_plus(settings.db_user)
    pwd = quote_plus(settings.db_password)
    server = settings.db_server

    params = f"driver={driver}&Encrypt={settings.db_encrypt}&TrustServerCertificate={settings.db_trust_cert}"
    url = f"mssql+pyodbc://{user}:{pwd}@{server}/{database}?{params}"
    return create_engine(url, pool_pre_ping=True)

def engines_do_dbo():
    return {db: ativar_engine(db) for db in settings.databases()}
