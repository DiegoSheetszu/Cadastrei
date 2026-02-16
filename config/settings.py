from pydantic import BaseModel, Field, ValidationError
from dotenv import load_dotenv
from pathlib import Path
import sys
import os


def _resolver_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent

        candidatos: list[Path] = []
        vistos: set[Path] = set()

        def _add(path: Path) -> None:
            p = path.resolve()
            if p not in vistos:
                vistos.add(p)
                candidatos.append(p)

        # 1) AppDirectory configurado no NSSM costuma ser o cwd do processo.
        _add(Path.cwd())
        # 2) Pasta do executavel.
        _add(exe_dir)
        # 3) Pais da pasta do executavel (ate a raiz), para suportar layout:
        # C:\Cadastrei\apps\prod\CadastreiMotoristasProd\CadastreiMotoristasProd.exe
        for parent in exe_dir.parents:
            _add(parent)

        for base in candidatos:
            if (base / ".env").exists():
                return base

        # Fallback final para manter previsibilidade em ambiente frozen.
        return Path.cwd()
    return Path(__file__).resolve().parents[1]


BASE_DIR = _resolver_base_dir()
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
    api_base_url: str = Field(default="", alias="API_BASE_URL")
    api_timeout_seconds: float = Field(default=30.0, alias="API_TIMEOUT_SECONDS")
    api_motorista_endpoint: str = Field(default="/v1/motorista", alias="API_MOTORISTA_ENDPOINT")
    api_afastamento_endpoint: str = Field(default="/v1/afastamento", alias="API_AFASTAMENTO_ENDPOINT")
    api_sync_interval_seconds: int = Field(default=15, alias="API_SYNC_INTERVAL_SECONDS")
    api_sync_batch_size_motoristas: int = Field(default=100, alias="API_SYNC_BATCH_SIZE_MOTORISTAS")
    api_sync_batch_size_afastamentos: int = Field(default=100, alias="API_SYNC_BATCH_SIZE_AFASTAMENTOS")
    api_sync_max_tentativas: int = Field(default=10, alias="API_SYNC_MAX_TENTATIVAS")
    api_sync_lock_timeout_minutes: int = Field(default=15, alias="API_SYNC_LOCK_TIMEOUT_MINUTES")
    api_sync_retry_base_seconds: int = Field(default=60, alias="API_SYNC_RETRY_BASE_SECONDS")
    api_sync_retry_max_seconds: int = Field(default=3600, alias="API_SYNC_RETRY_MAX_SECONDS")
    api_default_cidade: str = Field(default="NAO INFORMADO", alias="API_DEFAULT_CIDADE")
    api_default_uf: str = Field(default="SC", alias="API_DEFAULT_UF")
    api_motorista_sindicato_nome: str = Field(default="", alias="API_MOTORISTA_SINDICATO_NOME")
    api_motorista_sindicato_cnpj: str = Field(default="", alias="API_MOTORISTA_SINDICATO_CNPJ")
    api_motorista_sindicato_cidade: str = Field(default="", alias="API_MOTORISTA_SINDICATO_CIDADE")
    api_motorista_sindicato_uf: str = Field(default="", alias="API_MOTORISTA_SINDICATO_UF")

    source_database_dev: str = Field(default="Vetorh_Hom", alias="SOURCE_DATABASE_DEV")
    source_database_prod: str = Field(default="Vetorh_Prod", alias="SOURCE_DATABASE_PROD")
    source_schema_dev: str = Field(default="dbo", alias="SOURCE_SCHEMA_DEV")
    source_schema_prod: str = Field(default="Vetorh_Prod", alias="SOURCE_SCHEMA_PROD")

    target_database: str = Field(default="Cadastrei", alias="TARGET_DATABASE")
    target_schema: str = Field(default="dbo", alias="TARGET_SCHEMA")
    target_motorista_table: str = Field(default="MotoristaCadastro", alias="TARGET_MOTORISTA_TABLE")
    target_afastamento_table: str = Field(default="Afastamento", alias="TARGET_AFASTAMENTO_TABLE")
    motorista_sync_interval_seconds: int = Field(default=30, alias="MOTORISTA_SYNC_INTERVAL_SECONDS")
    motorista_sync_batch_size: int = Field(default=500, alias="MOTORISTA_SYNC_BATCH_SIZE")
    afastamento_sync_interval_seconds: int = Field(default=30, alias="AFASTAMENTO_SYNC_INTERVAL_SECONDS")
    afastamento_sync_batch_size: int = Field(default=500, alias="AFASTAMENTO_SYNC_BATCH_SIZE")
    afastamento_sync_data_inicio: str = Field(default="", alias="AFASTAMENTO_SYNC_DATA_INICIO")
    win_service_motoristas_dev: str = Field(default="CadastreiMotoristasHom", alias="WIN_SERVICE_MOTORISTAS_DEV")
    win_service_motoristas_prod: str = Field(default="CadastreiMotoristasProd", alias="WIN_SERVICE_MOTORISTAS_PROD")
    win_service_afastamentos_dev: str = Field(default="CadastreiAfastamentosHom", alias="WIN_SERVICE_AFASTAMENTOS_DEV")
    win_service_afastamentos_prod: str = Field(default="CadastreiAfastamentosProd", alias="WIN_SERVICE_AFASTAMENTOS_PROD")

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
