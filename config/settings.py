from pydantic import BaseModel, Field
from dotenv import load_dotenv
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH, override=True)


class Settings(BaseModel):
    db_server: str = Field(alias="DB_SERVER")
    db_user: str = Field(alias="DB_USER")
    db_password: str = Field(alias="DB_PASSWORD")

    db_databases: str = Field(default="master", alias="DB_DATABASES")
    db_driver: str = Field(default="ODBC Driver 17 for SQL Server", alias="DB_DRIVER")
    db_encrypt: str = Field(default="yes", alias="DB_ENCRYPT")
    db_trust_cert: str = Field(default="yes", alias="DB_TRUST_CERT")

    api_login_url: str = Field(default="", alias="API_LOGIN_URL")
    api_user: str = Field(default="", alias="API_USER")
    api_pass: str = Field(default="", alias="API_PASS")

    def databases(self) -> list[str]:
        return [x.strip() for x in self.db_databases.split(",") if x.strip()]


settings = Settings(**os.environ)
