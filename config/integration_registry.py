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
    de_para: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "tipo": self.tipo,
            "endpoint": self.endpoint,
            "tabela_destino": self.tabela_destino,
            "ativo": self.ativo,
            "de_para": [dict(item) for item in (self.de_para or []) if isinstance(item, dict)],
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
                de_para = self._sanitize_de_para(ep.get("de_para"))
                clean_eps.append(
                    {
                        "id": str(ep.get("id") or "").strip() or str(uuid.uuid4()),
                        "tipo": tipo_value,
                        "endpoint": endpoint_value,
                        "tabela_destino": str(ep.get("tabela_destino") or "").strip(),
                        "ativo": bool(ep.get("ativo", True)),
                        "de_para": de_para,
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
            nome="ATS (Padrao .env)",
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
                    de_para=self._default_de_para_motoristas(),
                ),
                IntegracaoEndpoint(
                    id=str(uuid.uuid4()),
                    tipo="afastamentos",
                    endpoint=str(settings.api_afastamento_endpoint or "").strip(),
                    tabela_destino=str(settings.target_afastamento_table or "").strip(),
                    de_para=self._default_de_para_afastamentos(),
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
                        "de_para": IntegracaoRegistry._default_de_para_motoristas(),
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
                        "de_para": IntegracaoRegistry._default_de_para_afastamentos(),
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
                    de_para=IntegracaoRegistry._sanitize_de_para(ep.get("de_para")),
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

    @staticmethod
    def _sanitize_de_para(raw_rules: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_rules, list):
            return []

        result: list[dict[str, Any]] = []
        for rule in raw_rules:
            if not isinstance(rule, dict):
                continue

            origem = str(rule.get("origem") or rule.get("source") or rule.get("from") or "").strip()
            destino = str(rule.get("destino") or rule.get("target") or rule.get("to") or "").strip()
            if not destino:
                continue

            clean_rule: dict[str, Any] = {
                "origem": origem,
                "destino": destino,
                "obrigatorio": bool(rule.get("obrigatorio", rule.get("required", False))),
                "ativo": bool(rule.get("ativo", rule.get("enabled", True))),
            }

            if "padrao" in rule:
                clean_rule["padrao"] = rule.get("padrao")
            elif "default" in rule:
                clean_rule["padrao"] = rule.get("default")

            transformacao = str(rule.get("transformacao") or rule.get("transform") or "").strip()
            if transformacao:
                clean_rule["transformacao"] = transformacao

            result.append(clean_rule)

        return result

    @staticmethod
    def _default_de_para_motoristas() -> list[dict[str, Any]]:
        return [
            {"origem": "payload.nome", "destino": "nome", "obrigatorio": True, "ativo": True},
            {"origem": "payload.cpf", "destino": "cpf", "obrigatorio": True, "ativo": True},
            {"origem": "payload.datanascimento", "destino": "datanascimento", "obrigatorio": False, "ativo": True},
            {"origem": "payload.genero", "destino": "genero", "obrigatorio": False, "ativo": True},
            {"origem": "payload.endereco.rua", "destino": "endereco.rua", "obrigatorio": False, "ativo": True, "padrao": "NAO INFORMADO"},
            {"origem": "payload.endereco.numero", "destino": "endereco.numero", "obrigatorio": False, "ativo": True, "padrao": "SN"},
            {"origem": "payload.endereco.complemento", "destino": "endereco.complemento", "obrigatorio": False, "ativo": True},
            {"origem": "payload.endereco.bairro", "destino": "endereco.bairro", "obrigatorio": False, "ativo": True, "padrao": "NAO INFORMADO"},
            {"origem": "payload.endereco.cidade", "destino": "endereco.cidade", "obrigatorio": True, "ativo": True, "padrao": "NAO INFORMADO"},
            {"origem": "payload.endereco.uf", "destino": "endereco.uf", "obrigatorio": True, "ativo": True, "transformacao": "upper", "padrao": "SC"},
            {"origem": "payload.endereco.cep", "destino": "endereco.cep", "obrigatorio": False, "ativo": True, "padrao": "00000000"},
            {"origem": "payload.endereco.latitude", "destino": "endereco.latitude", "obrigatorio": False, "ativo": True, "padrao": 0},
            {"origem": "payload.endereco.longitude", "destino": "endereco.longitude", "obrigatorio": False, "ativo": True, "padrao": 0},
            {"origem": "payload.dataadmissao", "destino": "dataadmissao", "obrigatorio": True, "ativo": True},
            {"origem": "payload.matricula", "destino": "matricula", "obrigatorio": True, "ativo": True, "transformacao": "str"},
        ]

    @staticmethod
    def _default_de_para_afastamentos() -> list[dict[str, Any]]:
        return [
            {"origem": "payload.numerodaempresa", "destino": "numerodaempresa", "obrigatorio": False, "ativo": True},
            {"origem": "payload.tipodecolaborador", "destino": "tipodecolaborador", "obrigatorio": False, "ativo": True},
            {"origem": "payload.numerodeorigemdocolaborador", "destino": "numerodeorigemdocolaborador", "obrigatorio": False, "ativo": True},
            {"origem": "payload.cpf", "destino": "cpf", "obrigatorio": True, "ativo": True},
            {"origem": "payload.descricao", "destino": "descricao", "obrigatorio": True, "ativo": True},
            {"origem": "payload.descricaodasituacao", "destino": "descricaodasituacao", "obrigatorio": False, "ativo": True},
            {"origem": "payload.datainicio", "destino": "datainicio", "obrigatorio": True, "ativo": True},
            {"origem": "payload.dataafastamento", "destino": "dataafastamento", "obrigatorio": False, "ativo": True},
            {"origem": "payload.horadoafastamento", "destino": "horadoafastamento", "obrigatorio": False, "ativo": True},
            {"origem": "payload.datatermino", "destino": "datatermino", "obrigatorio": False, "ativo": True},
            {"origem": "payload.horadotermino", "destino": "horadotermino", "obrigatorio": False, "ativo": True},
            {"origem": "payload.situacao", "destino": "situacao", "obrigatorio": False, "ativo": True},
            {"origem": "payload.rescisao", "destino": "rescisao", "obrigatorio": False, "ativo": True},
        ]
