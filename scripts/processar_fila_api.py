import argparse
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
        from src.integradora.api_dispatch_service import ApiDispatchService
    except Exception as exc:
        raise SystemExit(
            "Falha ao carregar configuracao. "
            "Defina DB_SERVER/DB_USER/DB_PASSWORD no .env ou passe via CLI "
            "(--db-server --db-user --db-password). "
            f"Detalhe: {exc}"
        )

    parser = argparse.ArgumentParser(
        description="Processador da fila de integracao API para MotoristaCadastro/Afastamento"
    )
    parser.add_argument("--destino-db", default=settings.target_database)
    parser.add_argument("--schema-destino", default=settings.target_schema)
    parser.add_argument("--tabela-motorista", default=settings.target_motorista_table)
    parser.add_argument("--tabela-afastamento", default=settings.target_afastamento_table)
    parser.add_argument("--endpoint-motorista", default=settings.api_motorista_endpoint)
    parser.add_argument("--endpoint-afastamento", default=settings.api_afastamento_endpoint)
    parser.add_argument("--intervalo", type=int, default=settings.api_sync_interval_seconds)
    parser.add_argument("--batch-motoristas", type=int, default=settings.api_sync_batch_size_motoristas)
    parser.add_argument("--batch-afastamentos", type=int, default=settings.api_sync_batch_size_afastamentos)
    parser.add_argument("--max-tentativas", type=int, default=settings.api_sync_max_tentativas)
    parser.add_argument("--lock-timeout-min", type=int, default=settings.api_sync_lock_timeout_minutes)
    parser.add_argument("--retry-base-sec", type=int, default=settings.api_sync_retry_base_seconds)
    parser.add_argument("--retry-max-sec", type=int, default=settings.api_sync_retry_max_seconds)
    parser.add_argument("--timeout-api", type=float, default=settings.api_timeout_seconds)
    parser.add_argument("--uma-vez", action="store_true")
    args = parser.parse_args(argv)

    engine_destino = ativar_engine((args.destino_db or "").strip() or settings.target_database)

    service = ApiDispatchService(
        engine_destino=engine_destino,
        schema_destino=(args.schema_destino or "").strip() or settings.target_schema,
        tabela_motorista=(args.tabela_motorista or "").strip() or settings.target_motorista_table,
        tabela_afastamento=(args.tabela_afastamento or "").strip() or settings.target_afastamento_table,
        endpoint_motorista=(args.endpoint_motorista or "").strip() or settings.api_motorista_endpoint,
        endpoint_afastamento=(args.endpoint_afastamento or "").strip() or settings.api_afastamento_endpoint,
        batch_size_motoristas=args.batch_motoristas,
        batch_size_afastamentos=args.batch_afastamentos,
        max_tentativas=args.max_tentativas,
        lock_timeout_minutes=args.lock_timeout_min,
        retry_base_seconds=args.retry_base_sec,
        retry_max_seconds=args.retry_max_sec,
        api_timeout_seconds=args.timeout_api,
    )

    try:
        if args.uma_vez:
            resultado = service.executar_ciclo()
            print(
                "Ciclo API concluido:",
                f"LockM={resultado.locks_liberados_motoristas}",
                f"LockA={resultado.locks_liberados_afastamentos}",
                f"CapM={resultado.motoristas_capturados}",
                f"OkM={resultado.motoristas_sucesso}",
                f"ErrM={resultado.motoristas_erro}",
                f"CapA={resultado.afastamentos_capturados}",
                f"OkA={resultado.afastamentos_sucesso}",
                f"ErrA={resultado.afastamentos_erro}",
            )
            return

        print(
            "Servico API iniciado.",
            f"destino={args.destino_db}.{args.schema_destino}",
            f"motoristas={args.tabela_motorista}",
            f"afastamentos={args.tabela_afastamento}",
            f"endpoint_m={args.endpoint_motorista}",
            f"endpoint_a={args.endpoint_afastamento}",
            f"intervalo={max(1, args.intervalo)}s",
            f"lote_m={max(1, args.batch_motoristas)}",
            f"lote_a={max(1, args.batch_afastamentos)}",
            f"max_tentativas={max(1, args.max_tentativas)}",
        )
        service.executar_continuo(intervalo_segundos=args.intervalo, logger=print)
    finally:
        service.close()


if __name__ == "__main__":
    main()
