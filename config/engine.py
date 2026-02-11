from sqlalchemy import create_engine
from config.settings import settings
from urllib.parse import quote_plus

def ativar_engine(database: str):

    driver = quote_plus(settings.db_driver)
    user = quote_plus(settings.db_user)
    pwd = quote_plus(settings.db_password)
    server = settings.db_server

    params = f"driver={driver}&Encrypt={settings.db_encrypt}&TrustServerCertificate={settings.db_trust_cert}"
    url = f"mssql+pyodbc://{user}:{pwd}@{server}/{database}?{params}"
    return create_engine(url, pool_pre_ping=True)

def engines_do_dbo():
    return {db: ativar_engine(db) for db in settings.databases()}
