import argparse
from datetime import datetime
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


def _logger_com_arquivo(log_file: Path):
    log_file.parent.mkdir(parents=True, exist_ok=True)

    def _log(message: str) -> None:
        timestamp = datetime.now().isoformat(sep=" ", timespec="seconds")
        line = f"[{timestamp}] {message}"
        print(line, flush=True)
        with log_file.open("a", encoding="utf-8") as fp:
            fp.write(line + "\n")

    return _log


def main() -> None:
    argv = _aplicar_overrides_de_conexao(sys.argv[1:])

    try:
        from config.engine import ativar_engine
        from config.settings import settings
        from src.integradora.api_dispatch_service import ApiDispatchService
    except Exception as exc:
        raise SystemExit(
            "Falha ao carregar configuracao. "
            "Defina DB_SERVER/DB_USER/DB_PASSWORD no .env ou passe via CLI "
            "(--db-server --db-user --db-password). "
            f"Detalhe: {exc}"
        )

    parser = argparse.ArgumentParser(
        description="Servico continuo de envio da fila de motoristas para a API ATS"
    )
    parser.add_argument("--destino-db", default=settings.target_database)
    parser.add_argument("--schema-destino", default=settings.target_schema)
    parser.add_argument("--tabela-motorista", default=settings.target_motorista_table)
    parser.add_argument("--endpoint-motorista", default=settings.api_motorista_endpoint)
    parser.add_argument("--intervalo", type=int, default=settings.api_sync_interval_seconds)
    parser.add_argument("--batch-motoristas", type=int, default=settings.api_sync_batch_size_motoristas)
    parser.add_argument("--max-tentativas", type=int, default=settings.api_sync_max_tentativas)
    parser.add_argument("--lock-timeout-min", type=int, default=settings.api_sync_lock_timeout_minutes)
    parser.add_argument("--retry-base-sec", type=int, default=settings.api_sync_retry_base_seconds)
    parser.add_argument("--retry-max-sec", type=int, default=settings.api_sync_retry_max_seconds)
    parser.add_argument("--timeout-api", type=float, default=settings.api_timeout_seconds)
    parser.add_argument("--log-file", default="logs/api_motoristas.log")
    parser.add_argument("--uma-vez", action="store_true")
    args = parser.parse_args(argv)

    logger = _logger_com_arquivo(Path(args.log_file))

    engine_destino = ativar_engine((args.destino_db or "").strip() or settings.target_database)
    service = ApiDispatchService(
        engine_destino=engine_destino,
        schema_destino=(args.schema_destino or "").strip() or settings.target_schema,
        tabela_motorista=(args.tabela_motorista or "").strip() or settings.target_motorista_table,
        tabela_afastamento=settings.target_afastamento_table,
        endpoint_motorista=(args.endpoint_motorista or "").strip() or settings.api_motorista_endpoint,
        endpoint_afastamento=settings.api_afastamento_endpoint,
        batch_size_motoristas=args.batch_motoristas,
        batch_size_afastamentos=1,
        max_tentativas=args.max_tentativas,
        lock_timeout_minutes=args.lock_timeout_min,
        retry_base_seconds=args.retry_base_sec,
        retry_max_seconds=args.retry_max_sec,
        api_timeout_seconds=args.timeout_api,
        processar_motoristas=True,
        processar_afastamentos=False,
    )

    try:
        if args.uma_vez:
            resultado = service.executar_ciclo()
            logger(
                "Ciclo API Motoristas concluido: "
                f"LockM={resultado.locks_liberados_motoristas} "
                f"CapM={resultado.motoristas_capturados} "
                f"OkM={resultado.motoristas_sucesso} "
                f"ErrM={resultado.motoristas_erro}"
            )
            return

        logger(
            "Servico API Motoristas iniciado: "
            f"destino={args.destino_db}.{args.schema_destino}.{args.tabela_motorista} "
            f"endpoint={args.endpoint_motorista} "
            f"intervalo={max(1, args.intervalo)}s "
            f"lote={max(1, args.batch_motoristas)} "
            f"max_tentativas={max(1, args.max_tentativas)} "
            f"log={Path(args.log_file).resolve()}"
        )
        service.executar_continuo(intervalo_segundos=args.intervalo, logger=logger)
    finally:
        service.close()


if __name__ == "__main__":
    main()
