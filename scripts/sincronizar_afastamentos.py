import argparse
from datetime import date
import os
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


def _aplicar_overrides_de_conexao(argv: list[str]) -> list[str]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db-server")
    parser.add_argument("--db-user")
    parser.add_argument("--db-password")
    parser.add_argument("--db-driver")
    parser.add_argument("--db-encrypt")
    parser.add_argument("--db-trust-cert")
    args, restantes = parser.parse_known_args(argv)

    mapping = {
        "DB_SERVER": args.db_server,
        "DB_USER": args.db_user,
        "DB_PASSWORD": args.db_password,
        "DB_DRIVER": args.db_driver,
        "DB_ENCRYPT": args.db_encrypt,
        "DB_TRUST_CERT": args.db_trust_cert,
    }
    for chave, valor in mapping.items():
        if valor is not None:
            os.environ[chave] = valor

    return restantes


def main() -> None:
    argv = _aplicar_overrides_de_conexao(sys.argv[1:])

    try:
        from config.engine import ativar_engine
        from config.settings import settings
        from src.integradora.afastamento_sync_service import AfastamentoSyncService
    except Exception as exc:
        raise SystemExit(
            "Falha ao carregar configuracao. "
            "Defina DB_SERVER/DB_USER/DB_PASSWORD no .env ou passe via CLI "
            "(--db-server --db-user --db-password). "
            f"Detalhe: {exc}"
        )

    parser = argparse.ArgumentParser(
        description="Servico de sincronizacao automatica de afastamentos para dbo.Afastamento"
    )
    parser.add_argument("--origem-db", default=settings.source_database_dev)
    parser.add_argument("--destino-db", default=settings.target_database)
    parser.add_argument("--schema-origem", default="")
    parser.add_argument("--intervalo", type=int, default=settings.afastamento_sync_interval_seconds)
    parser.add_argument("--batch-size", type=int, default=settings.afastamento_sync_batch_size)
    parser.add_argument(
        "--data-inicio",
        default=(settings.afastamento_sync_data_inicio.strip() or date.today().isoformat()),
        help="Data minima (YYYY-MM-DD) para processar afastamentos.",
    )
    parser.add_argument("--uma-vez", action="store_true")
    parser.add_argument("--reset-sync-state", action="store_true")
    args = parser.parse_args(argv)

    origem_db = (args.origem_db or "").strip() or settings.source_database_dev
    destino_db = (args.destino_db or "").strip() or settings.target_database
    schema_origem = (args.schema_origem or "").strip() or settings.source_schema_for_database(origem_db)

    engine_origem = ativar_engine(origem_db)
    engine_destino = ativar_engine(destino_db)

    service = AfastamentoSyncService(
        engine_origem=engine_origem,
        engine_destino=engine_destino,
        database_origem=origem_db,
        schema_origem=schema_origem,
        schema_destino=settings.target_schema,
        tabela_destino=settings.target_afastamento_table,
        batch_size=args.batch_size,
        data_inicio=args.data_inicio,
    )

    if args.reset_sync_state:
        service.resetar_estado_sync()
        print(f"Estado de sincronizacao resetado para origem={origem_db}.")

    if args.uma_vez:
        resultado = service.executar_ciclo()
        print(
            "Ciclo concluido:",
            f"Lidos={resultado.registros_origem}",
            f"Payload={resultado.payloads_validos}",
            f"Eventos={resultado.eventos_gerados}",
            f"Inseridos={resultado.eventos_inseridos}",
            f"ResetCursor={resultado.cursor_reiniciado}",
        )
        return

    print(
        f"Servico iniciado. origem={origem_db} schema={schema_origem} "
        f"destino={destino_db}.{settings.target_schema}.{settings.target_afastamento_table} "
        f"intervalo={max(1, args.intervalo)}s batch={max(1, args.batch_size)} "
        f"data_inicio={args.data_inicio}"
    )
    service.executar_continuo(intervalo_segundos=args.intervalo, logger=print)


if __name__ == "__main__":
    main()
