from pydantic import BaseModel, Field, ValidationError
from dotenv import load_dotenv
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH, override=False)


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

    source_database_dev: str = Field(default="Vetorh_Hom", alias="SOURCE_DATABASE_DEV")
    source_database_prod: str = Field(default="Vetorh_Prod", alias="SOURCE_DATABASE_PROD")
    source_schema_dev: str = Field(default="dbo", alias="SOURCE_SCHEMA_DEV")
    source_schema_prod: str = Field(default="Vetorh_Prod", alias="SOURCE_SCHEMA_PROD")

    target_database: str = Field(default="Cadastrei", alias="TARGET_DATABASE")
    target_schema: str = Field(default="dbo", alias="TARGET_SCHEMA")
    target_motorista_table: str = Field(default="MotoristaCadastro", alias="TARGET_MOTORISTA_TABLE")
    motorista_sync_interval_seconds: int = Field(default=30, alias="MOTORISTA_SYNC_INTERVAL_SECONDS")
    motorista_sync_batch_size: int = Field(default=500, alias="MOTORISTA_SYNC_BATCH_SIZE")

    def databases(self) -> list[str]:
        return [x.strip() for x in self.db_databases.split(",") if x.strip()]

    def source_schema_for_database(self, database: str) -> str:
        db_name = (database or "").strip().lower()
        if db_name == self.source_database_prod.lower():
            return self.source_schema_prod
        if db_name == self.source_database_dev.lower():
            return self.source_schema_dev
        return self.source_schema_dev


def _build_settings() -> Settings:
    try:
        return Settings(**os.environ)
    except ValidationError as exc:
        faltantes = []
        for erro in exc.errors():
            if erro.get("type") == "missing":
                loc = erro.get("loc") or []
                if loc:
                    faltantes.append(str(loc[0]))

        if faltantes:
            itens = ", ".join(sorted(set(faltantes)))
            raise RuntimeError(
                f"Variaveis obrigatorias ausentes: {itens}. "
                f"Configure no arquivo {ENV_PATH} ou no ambiente do sistema."
            ) from exc
        raise


settings = _build_settings()
