import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

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


def _normalizar_tipo_endpoint(value: str) -> str:
    text = str(value or "").strip().lower()
    if "afast" in text:
        return "afastamentos"
    if "motor" in text:
        return "motoristas"
    return text


def _carregar_config_api(*, registry_file: str, cliente_id: str, usar_registry: bool) -> dict[str, Any] | None:
    if not usar_registry:
        return None

    from config.integration_registry import IntegracaoRegistry

    registry_path = None
    if str(registry_file or "").strip():
        registry_path = Path(registry_file).expanduser().resolve()

    registry = IntegracaoRegistry(path=registry_path)
    cliente_id = str(cliente_id or "").strip()
    if cliente_id:
        for item in registry.list_configs():
            if item.id == cliente_id:
                return item.to_runtime_dict()
        raise ValueError(f"Cliente/API nao encontrado no registry: {cliente_id}")

    active = registry.get_active()
    return active.to_runtime_dict() if active else None


def _listar_endpoints_afastamentos(
    cfg: dict[str, Any] | None,
    *,
    endpoint_override: str,
    endpoint_id: str,
    endpoint_padrao: str,
    tabela_padrao: str,
) -> list[dict[str, Any]]:
    forced_endpoint = str(endpoint_override or "").strip()
    if forced_endpoint:
        return [
            {
                "id": "override_cli",
                "tipo": "afastamentos",
                "tipo_normalizado": "afastamentos",
                "endpoint": forced_endpoint,
                "tabela_destino": tabela_padrao,
                "de_para": [],
            }
        ]

    requested_endpoint_id = str(endpoint_id or "").strip()
    runtime_cfg = cfg or {}
    endpoints = runtime_cfg.get("endpoints") or []
    result: list[dict[str, Any]] = []

    for ep in endpoints:
        if not isinstance(ep, dict):
            continue

        ep_id = str(ep.get("id") or "").strip()
        if requested_endpoint_id and ep_id != requested_endpoint_id:
            continue

        if not bool(ep.get("ativo", True)):
            continue

        endpoint_path = str(ep.get("endpoint") or "").strip()
        tipo = str(ep.get("tipo") or "").strip()
        tipo_normalizado = _normalizar_tipo_endpoint(tipo)
        if tipo_normalizado not in {"afastamentos", "afastamento"}:
            continue
        if not endpoint_path:
            continue

        result.append(
            {
                "id": ep_id,
                "tipo": tipo,
                "tipo_normalizado": tipo_normalizado,
                "endpoint": endpoint_path,
                "tabela_destino": str(ep.get("tabela_destino") or "").strip(),
                "de_para": [dict(item) for item in (ep.get("de_para") or []) if isinstance(item, dict)],
            }
        )

    if result:
        return result

    if requested_endpoint_id:
        raise ValueError(
            f"Endpoint '{requested_endpoint_id}' nao encontrado/ativo para afastamentos no cliente/API selecionado."
        )

    if any(isinstance(ep, dict) for ep in endpoints):
        raise ValueError("Cliente/API ativo nao possui endpoint de afastamentos ativo.")

    legacy_endpoint = str(runtime_cfg.get("endpoint_afastamento") or "").strip()
    fallback_endpoint = legacy_endpoint or str(endpoint_padrao or "").strip()
    if not fallback_endpoint:
        raise ValueError("Nenhum endpoint de afastamentos configurado (registry/.env).")

    return [
        {
            "id": "fallback_env",
            "tipo": "afastamentos",
            "tipo_normalizado": "afastamentos",
            "endpoint": fallback_endpoint,
            "tabela_destino": tabela_padrao,
            "de_para": [],
        }
    ]


def _executar_ciclo_por_endpoints(
    *,
    engine_destino: Any,
    schema_destino: str,
    tabela_afastamento: str,
    endpoint_padrao: str,
    batch_afastamentos: int,
    max_tentativas: int,
    lock_timeout_min: int,
    retry_base_sec: int,
    retry_max_sec: int,
    timeout_api: float,
    cfg_api: dict[str, Any] | None,
    endpoint_override: str,
    endpoint_id: str,
    logger,
) -> dict[str, int]:
    from config.settings import settings
    from src.integradora.api_dispatch_service import ApiDispatchService

    endpoints = _listar_endpoints_afastamentos(
        cfg_api,
        endpoint_override=endpoint_override,
        endpoint_id=endpoint_id,
        endpoint_padrao=endpoint_padrao,
        tabela_padrao=tabela_afastamento,
    )

    timeout_seconds = float((cfg_api or {}).get("timeout_seconds") or timeout_api)

    total = {
        "endpoints": 0,
        "locks": 0,
        "capturados": 0,
        "sucesso": 0,
        "erro": 0,
    }

    for ep in endpoints:
        tabela_destino = str(ep.get("tabela_destino") or "").strip() or tabela_afastamento
        de_para = [dict(item) for item in (ep.get("de_para") or []) if isinstance(item, dict)]

        service = ApiDispatchService(
            engine_destino=engine_destino,
            schema_destino=schema_destino,
            tabela_motorista=settings.target_motorista_table,
            tabela_afastamento=tabela_destino,
            endpoint_motorista=settings.api_motorista_endpoint,
            endpoint_afastamento=str(ep.get("endpoint") or endpoint_padrao),
            batch_size_motoristas=1,
            batch_size_afastamentos=batch_afastamentos,
            max_tentativas=max_tentativas,
            lock_timeout_minutes=lock_timeout_min,
            retry_base_seconds=retry_base_sec,
            retry_max_seconds=retry_max_sec,
            api_timeout_seconds=timeout_seconds,
            processar_motoristas=False,
            processar_afastamentos=True,
            integration_config=cfg_api,
            payload_mapping=de_para,
        )

        try:
            resultado = service.executar_ciclo()
        finally:
            service.close()

        total["endpoints"] += 1
        total["locks"] += int(resultado.locks_liberados_afastamentos or 0)
        total["capturados"] += int(resultado.afastamentos_capturados or 0)
        total["sucesso"] += int(resultado.afastamentos_sucesso or 0)
        total["erro"] += int(resultado.afastamentos_erro or 0)

        logger(
            "[API Afastamentos] "
            f"endpoint_id={ep.get('id') or '-'} "
            f"path={ep.get('endpoint')} "
            f"de_para={len(de_para)} "
            f"CapA={resultado.afastamentos_capturados} "
            f"OkA={resultado.afastamentos_sucesso} "
            f"ErrA={resultado.afastamentos_erro}"
        )

    return total


def main() -> None:
    argv = _aplicar_overrides_de_conexao(sys.argv[1:])

    try:
        from config.engine import ativar_engine
        from config.settings import settings
    except Exception as exc:
        raise SystemExit(
            "Falha ao carregar configuracao. "
            "Defina DB_SERVER/DB_USER/DB_PASSWORD no .env ou passe via CLI "
            "(--db-server --db-user --db-password). "
            f"Detalhe: {exc}"
        )

    parser = argparse.ArgumentParser(
        description="Servico continuo de envio da fila de afastamentos para API com endpoints dinamicos"
    )
    parser.add_argument("--destino-db", default=settings.target_database)
    parser.add_argument("--schema-destino", default=settings.target_schema)
    parser.add_argument("--tabela-afastamento", default=settings.target_afastamento_table)
    parser.add_argument(
        "--endpoint-afastamento",
        default="",
        help="Forca endpoint unico para afastamentos (ignora endpoint cadastrado no registry).",
    )
    parser.add_argument(
        "--endpoint-id",
        default="",
        help="Processa apenas um endpoint pelo ID cadastrado no clientes_api.json.",
    )
    parser.add_argument(
        "--cliente-id",
        default="",
        help="Usa um cliente/API especifico do clientes_api.json; vazio usa active_id.",
    )
    parser.add_argument(
        "--registry-file",
        default="",
        help="Caminho alternativo do clientes_api.json.",
    )
    parser.add_argument(
        "--sem-registry",
        action="store_true",
        help="Nao usa clientes_api.json; usa apenas endpoint informado/.env.",
    )
    parser.add_argument("--intervalo", type=int, default=settings.api_sync_interval_seconds)
    parser.add_argument("--batch-afastamentos", type=int, default=settings.api_sync_batch_size_afastamentos)
    parser.add_argument("--max-tentativas", type=int, default=settings.api_sync_max_tentativas)
    parser.add_argument("--lock-timeout-min", type=int, default=settings.api_sync_lock_timeout_minutes)
    parser.add_argument("--retry-base-sec", type=int, default=settings.api_sync_retry_base_seconds)
    parser.add_argument("--retry-max-sec", type=int, default=settings.api_sync_retry_max_seconds)
    parser.add_argument("--timeout-api", type=float, default=settings.api_timeout_seconds)
    parser.add_argument("--log-file", default="logs/api_afastamentos.log")
    parser.add_argument("--uma-vez", action="store_true")
    args = parser.parse_args(argv)

    logger = _logger_com_arquivo(Path(args.log_file))
    engine_destino = ativar_engine((args.destino_db or "").strip() or settings.target_database)
    intervalo = max(1, int(args.intervalo))

    def _rodar_um_ciclo() -> dict[str, int]:
        cfg_api = _carregar_config_api(
            registry_file=args.registry_file,
            cliente_id=args.cliente_id,
            usar_registry=not bool(args.sem_registry),
        )
        return _executar_ciclo_por_endpoints(
            engine_destino=engine_destino,
            schema_destino=(args.schema_destino or "").strip() or settings.target_schema,
            tabela_afastamento=(args.tabela_afastamento or "").strip() or settings.target_afastamento_table,
            endpoint_padrao=str(settings.api_afastamento_endpoint or "").strip(),
            batch_afastamentos=max(1, int(args.batch_afastamentos)),
            max_tentativas=max(1, int(args.max_tentativas)),
            lock_timeout_min=max(1, int(args.lock_timeout_min)),
            retry_base_sec=max(1, int(args.retry_base_sec)),
            retry_max_sec=max(1, int(args.retry_max_sec)),
            timeout_api=max(1.0, float(args.timeout_api)),
            cfg_api=cfg_api,
            endpoint_override=(args.endpoint_afastamento or "").strip(),
            endpoint_id=(args.endpoint_id or "").strip(),
            logger=logger,
        )

    if args.uma_vez:
        resumo = _rodar_um_ciclo()
        logger(
            "Ciclo API Afastamentos concluido: "
            f"Endpoints={resumo['endpoints']} "
            f"LockA={resumo['locks']} "
            f"CapA={resumo['capturados']} "
            f"OkA={resumo['sucesso']} "
            f"ErrA={resumo['erro']}"
        )
        return

    logger(
        "Servico API Afastamentos iniciado: "
        f"destino={args.destino_db}.{args.schema_destino}.{args.tabela_afastamento} "
        f"intervalo={intervalo}s "
        f"lote={max(1, int(args.batch_afastamentos))} "
        f"max_tentativas={max(1, int(args.max_tentativas))} "
        f"registry={'OFF' if args.sem_registry else 'ON'} "
        f"cliente_id={(args.cliente_id or '').strip() or 'active_id'} "
        f"endpoint_id={(args.endpoint_id or '').strip() or '-'} "
        f"endpoint_override={(args.endpoint_afastamento or '').strip() or '-'} "
        f"log={Path(args.log_file).resolve()}"
    )

    while True:
        started = time.time()
        try:
            resumo = _rodar_um_ciclo()
            logger(
                "Ciclo API Afastamentos: "
                f"Endpoints={resumo['endpoints']} "
                f"LockA={resumo['locks']} "
                f"CapA={resumo['capturados']} "
                f"OkA={resumo['sucesso']} "
                f"ErrA={resumo['erro']}"
            )
        except Exception as exc:
            logger(f"ERRO: {exc}")

        elapsed = time.time() - started
        sleep_for = intervalo - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
