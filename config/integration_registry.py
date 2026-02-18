from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any

from config.settings import BASE_DIR, settings


@dataclass
class IntegracaoEndpoint:
    id: str
    tipo: str
    endpoint: str
    tabela_destino: str
    ativo: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "tipo": self.tipo,
            "endpoint": self.endpoint,
            "tabela_destino": self.tabela_destino,
            "ativo": self.ativo,
        }


@dataclass
class IntegracaoClienteApi:
    id: str
    nome: str
    fornecedor: str
    base_url: str
    login_url: str
    usuario: str
    senha: str
    timeout_seconds: float
    endpoints: list[IntegracaoEndpoint] = field(default_factory=list)

    def to_runtime_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "nome": self.nome,
            "fornecedor": self.fornecedor,
            "base_url": self.base_url,
            "login_url": self.login_url,
            "usuario": self.usuario,
            "senha": self.senha,
            "timeout_seconds": self.timeout_seconds,
            "endpoints": [ep.to_dict() for ep in self.endpoints],
        }


class IntegracaoRegistry:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or (BASE_DIR / "clientes_api.json")
        self._lock = Lock()

    def list_configs(self) -> list[IntegracaoClienteApi]:
        data = self._read()
        items = data.get("items") or []
        result: list[IntegracaoClienteApi] = []
        for raw in items:
            try:
                result.append(self._from_dict(raw))
            except Exception:
                continue
        return result

    def get_active_id(self) -> str | None:
        data = self._read()
        value = str(data.get("active_id") or "").strip()
        return value or None

    def get_active(self) -> IntegracaoClienteApi | None:
        active_id = self.get_active_id()
        if not active_id:
            return None
        for item in self.list_configs():
            if item.id == active_id:
                return item
        return None

    def upsert(self, item: IntegracaoClienteApi) -> IntegracaoClienteApi:
        with self._lock:
            data = self._read()
            items = data.get("items") or []

            payload = asdict(item)
            payload["id"] = str(payload.get("id") or "").strip() or str(uuid.uuid4())
            payload["timeout_seconds"] = float(payload.get("timeout_seconds") or settings.api_timeout_seconds)

            eps = payload.get("endpoints") or []
            clean_eps: list[dict[str, Any]] = []
            for ep in eps:
                if not isinstance(ep, dict):
                    continue
                endpoint_value = str(ep.get("endpoint") or "").strip()
                tipo_value = str(ep.get("tipo") or "").strip()
                if not endpoint_value or not tipo_value:
                    continue
                clean_eps.append(
                    {
                        "id": str(ep.get("id") or "").strip() or str(uuid.uuid4()),
                        "tipo": tipo_value,
                        "endpoint": endpoint_value,
                        "tabela_destino": str(ep.get("tabela_destino") or "").strip(),
                        "ativo": bool(ep.get("ativo", True)),
                    }
                )
            payload["endpoints"] = clean_eps

            found = False
            for idx, existing in enumerate(items):
                if str(existing.get("id") or "") == payload["id"]:
                    items[idx] = payload
                    found = True
                    break
            if not found:
                items.append(payload)

            data["items"] = items
            if not data.get("active_id"):
                data["active_id"] = payload["id"]

            self._write(data)
            return self._from_dict(payload)

    def delete(self, item_id: str) -> None:
        key = str(item_id or "").strip()
        if not key:
            return

        with self._lock:
            data = self._read()
            items = data.get("items") or []
            items = [x for x in items if str(x.get("id") or "") != key]
            data["items"] = items

            if str(data.get("active_id") or "") == key:
                data["active_id"] = str(items[0].get("id") or "") if items else ""

            self._write(data)

    def set_active(self, item_id: str) -> None:
        key = str(item_id or "").strip()
        if not key:
            return

        with self._lock:
            data = self._read()
            ids = {str(x.get("id") or "") for x in (data.get("items") or [])}
            if key not in ids:
                raise ValueError("Cliente/API nao encontrado para ativacao.")
            data["active_id"] = key
            self._write(data)

    def default_config(self) -> IntegracaoClienteApi:
        return IntegracaoClienteApi(
            id="",
            nome="Padrao .env",
            fornecedor="ATS_Log",
            base_url=str(settings.api_base_url or "").strip(),
            login_url=str(settings.api_login_url or "").strip(),
            usuario=str(settings.api_user or "").strip(),
            senha=str(settings.api_pass or "").strip(),
            timeout_seconds=float(settings.api_timeout_seconds or 30.0),
            endpoints=[
                IntegracaoEndpoint(
                    id=str(uuid.uuid4()),
                    tipo="motoristas",
                    endpoint=str(settings.api_motorista_endpoint or "").strip(),
                    tabela_destino=str(settings.target_motorista_table or "").strip(),
                ),
                IntegracaoEndpoint(
                    id=str(uuid.uuid4()),
                    tipo="afastamentos",
                    endpoint=str(settings.api_afastamento_endpoint or "").strip(),
                    tabela_destino=str(settings.target_afastamento_table or "").strip(),
                ),
            ],
        )

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"active_id": "", "items": []}

        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                raw.setdefault("active_id", "")
                raw.setdefault("items", [])
                return raw
        except Exception:
            pass

        return {"active_id": "", "items": []}

    def _write(self, data: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _from_dict(raw: dict[str, Any]) -> IntegracaoClienteApi:
        endpoints_raw = raw.get("endpoints") or []

        # compatibilidade com estrutura antiga
        if not endpoints_raw:
            ep_m = str(raw.get("endpoint_motorista") or "").strip()
            ep_a = str(raw.get("endpoint_afastamento") or "").strip()
            if ep_m:
                endpoints_raw.append(
                    {
                        "id": str(uuid.uuid4()),
                        "tipo": "motoristas",
                        "endpoint": ep_m,
                        "tabela_destino": str(settings.target_motorista_table or "").strip(),
                        "ativo": True,
                    }
                )
            if ep_a:
                endpoints_raw.append(
                    {
                        "id": str(uuid.uuid4()),
                        "tipo": "afastamentos",
                        "endpoint": ep_a,
                        "tabela_destino": str(settings.target_afastamento_table or "").strip(),
                        "ativo": True,
                    }
                )

        endpoints: list[IntegracaoEndpoint] = []
        for ep in endpoints_raw:
            if not isinstance(ep, dict):
                continue
            endpoint_value = str(ep.get("endpoint") or "").strip()
            tipo_value = str(ep.get("tipo") or "").strip()
            if not endpoint_value or not tipo_value:
                continue
            endpoints.append(
                IntegracaoEndpoint(
                    id=str(ep.get("id") or "").strip() or str(uuid.uuid4()),
                    tipo=tipo_value,
                    endpoint=endpoint_value,
                    tabela_destino=str(ep.get("tabela_destino") or "").strip(),
                    ativo=bool(ep.get("ativo", True)),
                )
            )

        return IntegracaoClienteApi(
            id=str(raw.get("id") or "").strip() or str(uuid.uuid4()),
            nome=str(raw.get("nome") or "").strip(),
            fornecedor=str(raw.get("fornecedor") or "ATS_Log").strip() or "ATS_Log",
            base_url=str(raw.get("base_url") or "").strip(),
            login_url=str(raw.get("login_url") or "").strip(),
            usuario=str(raw.get("usuario") or "").strip(),
            senha=str(raw.get("senha") or "").strip(),
            timeout_seconds=float(raw.get("timeout_seconds") or 30.0),
            endpoints=endpoints,
        )
