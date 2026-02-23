from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Mapping

from sqlalchemy.engine import Engine

from Cadastro_API.client import ApiResponse, AtsApiClient
from Consultas_dbo.cadastrei.fila_integracao_api import RepositorioFilaIntegracaoApi
from config.settings import settings


@dataclass
class ResultadoCicloApi:
    locks_liberados_motoristas: int = 0
    locks_liberados_afastamentos: int = 0
    motoristas_capturados: int = 0
    motoristas_sucesso: int = 0
    motoristas_erro: int = 0
    afastamentos_capturados: int = 0
    afastamentos_sucesso: int = 0
    afastamentos_erro: int = 0


class ApiDispatchService:
    def __init__(
        self,
        *,
        engine_destino: Engine,
        schema_destino: str = "dbo",
        tabela_motorista: str = "MotoristaCadastro",
        tabela_afastamento: str = "Afastamento",
        endpoint_motorista: str | None = None,
        endpoint_afastamento: str | None = None,
        batch_size_motoristas: int = 100,
        batch_size_afastamentos: int = 100,
        max_tentativas: int = 10,
        lock_timeout_minutes: int = 15,
        retry_base_seconds: int = 60,
        retry_max_seconds: int = 3600,
        api_timeout_seconds: float = 30.0,
        processar_motoristas: bool = True,
        processar_afastamentos: bool = True,
        integration_config: Mapping[str, Any] | None = None,
        payload_mapping: list[dict[str, Any]] | None = None,
    ) -> None:
        self.processar_motoristas = bool(processar_motoristas)
        self.processar_afastamentos = bool(processar_afastamentos)
        if not self.processar_motoristas and not self.processar_afastamentos:
            raise ValueError("ApiDispatchService precisa processar pelo menos um tipo de fila.")

        self.batch_size_motoristas = max(1, int(batch_size_motoristas))
        self.batch_size_afastamentos = max(1, int(batch_size_afastamentos))
        self.max_tentativas = max(1, int(max_tentativas))
        self.lock_timeout_minutes = max(1, int(lock_timeout_minutes))
        self.retry_base_seconds = max(1, int(retry_base_seconds))
        self.retry_max_seconds = max(self.retry_base_seconds, int(retry_max_seconds))
        self.integration_config = dict(integration_config or {})
        self.cliente_api_id = str(self.integration_config.get("id") or "").strip() or None
        self.ambiente_api = str(self.integration_config.get("ambiente") or "").strip().upper() or None
        self.endpoint_motorista = (
            endpoint_motorista
            or str(self.integration_config.get("endpoint_motorista") or settings.api_motorista_endpoint)
        )
        self.endpoint_afastamento = (
            endpoint_afastamento
            or str(self.integration_config.get("endpoint_afastamento") or settings.api_afastamento_endpoint)
        )
        mapping_raw = payload_mapping
        if mapping_raw is None:
            mapping_raw = self.integration_config.get("de_para")  # type: ignore[assignment]
        self.payload_mapping = self._normalizar_de_para(mapping_raw)
        self.colunas_origem_de_para = self._extrair_colunas_origem(self.payload_mapping)
        timeout_api = float(self.integration_config.get("timeout_seconds") or api_timeout_seconds)

        self.repo = RepositorioFilaIntegracaoApi(
            engine_destino,
            schema=schema_destino,
            tabela_motorista=tabela_motorista,
            tabela_afastamento=tabela_afastamento,
        )
        self.api_client = AtsApiClient(timeout_seconds=timeout_api, integration_config=self.integration_config)

    def close(self) -> None:
        self.api_client.close()

    def executar_ciclo(self) -> ResultadoCicloApi:
        resultado = ResultadoCicloApi()

        if self.processar_motoristas:
            resultado.locks_liberados_motoristas = self.repo.liberar_locks_expirados_motoristas(
                lock_timeout_minutes=self.lock_timeout_minutes
            )
            lock_id_motoristas = str(uuid.uuid4())
            eventos_motoristas = self.repo.capturar_motoristas_pendentes(
                lock_id=lock_id_motoristas,
                batch_size=self.batch_size_motoristas,
                max_tentativas=self.max_tentativas,
                lock_timeout_minutes=self.lock_timeout_minutes,
            )
            resultado.motoristas_capturados = len(eventos_motoristas)
            for evento in eventos_motoristas:
                if self._processar_motorista(evento, lock_id_motoristas):
                    resultado.motoristas_sucesso += 1
                else:
                    resultado.motoristas_erro += 1

        if self.processar_afastamentos:
            resultado.locks_liberados_afastamentos = self.repo.liberar_locks_expirados_afastamentos(
                lock_timeout_minutes=self.lock_timeout_minutes
            )
            lock_id_afastamentos = str(uuid.uuid4())
            eventos_afastamentos = self.repo.capturar_afastamentos_pendentes(
                lock_id=lock_id_afastamentos,
                batch_size=self.batch_size_afastamentos,
                max_tentativas=self.max_tentativas,
                lock_timeout_minutes=self.lock_timeout_minutes,
            )
            resultado.afastamentos_capturados = len(eventos_afastamentos)
            for evento in eventos_afastamentos:
                if self._processar_afastamento(evento, lock_id_afastamentos):
                    resultado.afastamentos_sucesso += 1
                else:
                    resultado.afastamentos_erro += 1

        return resultado

    def executar_continuo(
        self,
        *,
        intervalo_segundos: int,
        logger: Callable[[str], None] | None = None,
        stop_event: Any | None = None,
    ) -> None:
        intervalo = max(1, int(intervalo_segundos))
        sink = logger or (lambda _: None)

        while True:
            if stop_event is not None and stop_event.is_set():
                break

            inicio = time.time()
            try:
                resultado = self.executar_ciclo()
                sink(
                    (
                        f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] "
                        f"LockM={resultado.locks_liberados_motoristas} "
                        f"LockA={resultado.locks_liberados_afastamentos} "
                        f"CapM={resultado.motoristas_capturados} "
                        f"OkM={resultado.motoristas_sucesso} "
                        f"ErrM={resultado.motoristas_erro} "
                        f"CapA={resultado.afastamentos_capturados} "
                        f"OkA={resultado.afastamentos_sucesso} "
                        f"ErrA={resultado.afastamentos_erro}"
                    )
                )
            except Exception as exc:
                sink(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] ERRO: {exc}")

            elapsed = time.time() - inicio
            sleep_for = intervalo - elapsed
            if sleep_for > 0:
                if stop_event is not None:
                    if stop_event.wait(sleep_for):
                        break
                else:
                    time.sleep(sleep_for)

    def _processar_motorista(self, evento: dict[str, Any], lock_id: str) -> bool:
        try:
            payload = self._carregar_payload(evento)
            if self.payload_mapping:
                colunas_origem = self.repo.buscar_colunas_motorista_por_evento(
                    evento=evento,
                    colunas=self.colunas_origem_de_para,
                )
                payload_origem = self._montar_origem_de_para(payload, evento=evento, colunas=colunas_origem)
                payload = self._aplicar_de_para(payload_origem, contexto="motoristas")
            payload = self._enriquecer_payload_motorista(payload)
            payload = self._enriquecer_payload_empregador(payload, evento=evento)
            payload = self._enriquecer_payload_sindicato(payload, evento=evento)
            self._validar_payload_motorista(payload)
            response = self.api_client.post_json(self.endpoint_motorista, payload)
        except Exception as exc:
            return self._registrar_erro_motorista(
                evento=evento,
                lock_id=lock_id,
                http_status=None,
                resposta_resumo=None,
                detalhe_erro=str(exc),
            )

        if self._resposta_indica_sucesso(response):
            return self.repo.marcar_motorista_sucesso(
                evento=evento,
                lock_id=lock_id,
                http_status=response.status_code,
                resposta_resumo=self._resumo_resposta(response),
            )

        return self._registrar_erro_motorista(
            evento=evento,
            lock_id=lock_id,
            http_status=response.status_code,
            resposta_resumo=self._resumo_resposta(response),
            detalhe_erro=self._mensagem_erro_resposta(response),
        )

    def _processar_afastamento(self, evento: dict[str, Any], lock_id: str) -> bool:
        try:
            payload = self._carregar_payload(evento)
            if self.payload_mapping:
                colunas_origem = self.repo.buscar_colunas_afastamento_por_evento(
                    evento=evento,
                    colunas=self.colunas_origem_de_para,
                )
                payload_origem = self._montar_origem_de_para(payload, evento=evento, colunas=colunas_origem)
                payload = self._aplicar_de_para(payload_origem, contexto="afastamentos")
            payload = self._enriquecer_payload_empregador(payload, evento=evento)
            self._validar_payload_afastamento(payload)
            response = self.api_client.post_json(self.endpoint_afastamento, payload)
        except Exception as exc:
            return self._registrar_erro_afastamento(
                evento=evento,
                lock_id=lock_id,
                http_status=None,
                resposta_resumo=None,
                detalhe_erro=str(exc),
            )

        if self._resposta_indica_sucesso(response):
            return self.repo.marcar_afastamento_sucesso(
                evento=evento,
                lock_id=lock_id,
                http_status=response.status_code,
                resposta_resumo=self._resumo_resposta(response),
            )

        return self._registrar_erro_afastamento(
            evento=evento,
            lock_id=lock_id,
            http_status=response.status_code,
            resposta_resumo=self._resumo_resposta(response),
            detalhe_erro=self._mensagem_erro_resposta(response),
        )

    def _registrar_erro_motorista(
        self,
        *,
        evento: dict[str, Any],
        lock_id: str,
        http_status: int | None,
        resposta_resumo: str | None,
        detalhe_erro: str,
    ) -> bool:
        tentativa_atual = int(evento.get("tentativas") or 0)
        proxima = self._calcular_proxima_tentativa(tentativa_atual + 1)
        return self.repo.marcar_motorista_erro(
            evento=evento,
            lock_id=lock_id,
            http_status=http_status,
            resposta_resumo=resposta_resumo,
            ultimo_erro=self._limitar_texto(detalhe_erro),
            proxima_tentativa=proxima,
        )

    def _registrar_erro_afastamento(
        self,
        *,
        evento: dict[str, Any],
        lock_id: str,
        http_status: int | None,
        resposta_resumo: str | None,
        detalhe_erro: str,
    ) -> bool:
        tentativa_atual = int(evento.get("tentativas") or 0)
        proxima = self._calcular_proxima_tentativa(tentativa_atual + 1)
        return self.repo.marcar_afastamento_erro(
            evento=evento,
            lock_id=lock_id,
            http_status=http_status,
            resposta_resumo=resposta_resumo,
            ultimo_erro=self._limitar_texto(detalhe_erro),
            proxima_tentativa=proxima,
        )

    def _calcular_proxima_tentativa(self, tentativas_apos_erro: int) -> datetime | None:
        if tentativas_apos_erro >= self.max_tentativas:
            return None

        fator = max(0, int(tentativas_apos_erro) - 1)
        delay = self.retry_base_seconds * (2 ** fator)
        delay = min(delay, self.retry_max_seconds)
        return datetime.utcnow() + timedelta(seconds=int(delay))

    @staticmethod
    def _carregar_payload(evento: dict[str, Any]) -> dict[str, Any]:
        payload_raw = evento.get("payload_json")
        if payload_raw is None:
            raise ValueError("Evento sem PayloadJson.")

        if isinstance(payload_raw, dict):
            return dict(payload_raw)

        try:
            parsed = json.loads(str(payload_raw))
        except Exception as exc:
            raise ValueError(f"PayloadJson invalido: {exc}") from exc

        if not isinstance(parsed, dict):
            raise ValueError("PayloadJson precisa representar um objeto JSON.")
        return parsed

    def _enriquecer_payload_motorista(self, payload: dict[str, Any]) -> dict[str, Any]:
        motorista = dict(payload)

        endereco = motorista.get("endereco")
        if not isinstance(endereco, dict):
            endereco = {}
        endereco = dict(endereco)
        endereco["cidade"] = str(endereco.get("cidade") or settings.api_default_cidade).strip()
        endereco["uf"] = str(endereco.get("uf") or settings.api_default_uf).strip().upper()
        motorista["endereco"] = endereco

        if "sindicato" not in motorista or not motorista.get("sindicato"):
            sindicato_padrao = self._sindicato_padrao()
            if sindicato_padrao:
                motorista["sindicato"] = sindicato_padrao

        sindicato = motorista.get("sindicato")
        if isinstance(sindicato, dict):
            sindicato = dict(sindicato)
            end_sindicato = sindicato.get("endereco")
            if not isinstance(end_sindicato, dict):
                end_sindicato = {}
            end_sindicato = dict(end_sindicato)
            end_sindicato["cidade"] = str(
                end_sindicato.get("cidade") or settings.api_default_cidade
            ).strip()
            end_sindicato["uf"] = str(
                end_sindicato.get("uf") or settings.api_default_uf
            ).strip().upper()
            sindicato["endereco"] = end_sindicato
            motorista["sindicato"] = sindicato

        return motorista

    def _enriquecer_payload_empregador(self, payload: dict[str, Any], *, evento: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(payload)

        codigo = self._resolver_codigo_empresa(enriched, evento)
        mapped = self._buscar_pessoa_juridica(codigo=codigo, tipo_pessoa="EMPREGADOR")
        fallback = self._empregador_padrao_por_codigo(codigo)

        atual_raw = enriched.get("empregador")
        atual = dict(atual_raw) if isinstance(atual_raw, dict) else {}
        merged = self._merge_pessoa_juridica(atual, mapped)
        merged = self._merge_pessoa_juridica(merged, fallback)

        if codigo and str(merged.get("codigo") or "").strip() == "":
            merged["codigo"] = codigo

        if merged:
            enriched["empregador"] = merged

        return enriched

    def _enriquecer_payload_sindicato(self, payload: dict[str, Any], *, evento: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(payload)

        codigo_sindicato = self._resolver_codigo_sindicato(enriched, evento)
        mapped = self._buscar_pessoa_juridica(codigo=codigo_sindicato, tipo_pessoa="SINDICATO")
        fallback = self._sindicato_padrao()

        atual_raw = enriched.get("sindicato")
        atual = dict(atual_raw) if isinstance(atual_raw, dict) else {}
        merged = self._merge_pessoa_juridica(atual, mapped)
        merged = self._merge_pessoa_juridica(merged, fallback)
        if codigo_sindicato and str(merged.get("codigo") or "").strip() == "":
            merged["codigo"] = codigo_sindicato

        if merged:
            enriched["sindicato"] = merged

        return enriched

    def _buscar_pessoa_juridica(self, *, codigo: str, tipo_pessoa: str) -> dict[str, Any] | None:
        codigo = str(codigo or "").strip()
        if not codigo:
            return None
        try:
            codigo_int = int(float(codigo.replace(",", ".")))
        except Exception:
            return None

        found = self.repo.buscar_pessoa_juridica_por_codigo(
            codigo_empresa=codigo_int,
            tipo_pessoa=tipo_pessoa,
            cliente_api_id=self.cliente_api_id,
            ambiente=self.ambiente_api,
        )
        if not isinstance(found, dict):
            return None

        nome = str(found.get("nome") or "").strip()
        cnpj = "".join(ch for ch in str(found.get("cnpj") or "") if ch.isdigit())
        cidade = str(found.get("cidade") or "").strip()
        uf = str(found.get("uf") or "").strip().upper()
        if not (nome and cnpj and cidade and uf):
            return None

        endereco: dict[str, Any] = {
            "cidade": cidade,
            "uf": uf,
        }
        for key in ("rua", "numero", "complemento", "bairro", "cep", "latitude", "longitude"):
            value = found.get(key)
            if value is None:
                continue
            text_value = str(value).strip()
            if text_value == "":
                continue
            endereco[key] = value

        codigo_pessoa = str(found.get("codigo_pessoa") or "").strip()
        codigo_final = codigo_pessoa or str(codigo_int)

        result: dict[str, Any] = {
            "nome": nome,
            "cnpj": cnpj,
            "endereco": endereco,
        }
        result["codigo"] = codigo_final
        return result

    @staticmethod
    def _merge_pessoa_juridica(
        atual: dict[str, Any] | None,
        fonte: dict[str, Any] | None,
    ) -> dict[str, Any]:
        base = dict(atual or {})
        src = dict(fonte or {})
        if not src:
            return base

        for key in ("codigo", "nome", "cnpj"):
            if str(base.get(key) or "").strip() == "":
                value = src.get(key)
                if value is not None and str(value).strip() != "":
                    base[key] = value

        end_base_raw = base.get("endereco")
        end_base = dict(end_base_raw) if isinstance(end_base_raw, dict) else {}
        end_src_raw = src.get("endereco")
        end_src = dict(end_src_raw) if isinstance(end_src_raw, dict) else {}
        for key in ("rua", "numero", "complemento", "bairro", "cidade", "uf", "cep", "latitude", "longitude"):
            if key not in end_src:
                continue
            if str(end_base.get(key) or "").strip() == "":
                value = end_src.get(key)
                if value is not None and str(value).strip() != "":
                    end_base[key] = value
        if end_base:
            base["endereco"] = end_base
        return base

    @staticmethod
    def _resolver_codigo_empresa(payload: dict[str, Any], evento: dict[str, Any]) -> str:
        candidatos = [
            payload.get("empregador", {}).get("codigo")
            if isinstance(payload.get("empregador"), dict)
            else None,
            payload.get("codigoempresacontratante"),
            payload.get("codigo_empresa_contratante"),
            payload.get("numerodaempresa"),
            payload.get("numempresa"),
            payload.get("numemp"),
            evento.get("codigoempresacontratante"),
            evento.get("codigo_empresa_contratante"),
            evento.get("numempresa"),
            evento.get("numemp"),
            settings.api_empregador_codigo,
        ]

        for candidato in candidatos:
            if candidato is None:
                continue
            txt = str(candidato).strip()
            if txt:
                return txt
        return ""

    @staticmethod
    def _resolver_codigo_sindicato(payload: dict[str, Any], evento: dict[str, Any]) -> str:
        candidatos = [
            payload.get("sindicato", {}).get("codigo")
            if isinstance(payload.get("sindicato"), dict)
            else None,
            payload.get("numerosindicato"),
            payload.get("numero_sindicato"),
            payload.get("codigosindicato"),
            payload.get("codigo_sindicato"),
            evento.get("numero_sindicato"),
            evento.get("numerosindicato"),
            evento.get("codigosindicato"),
            evento.get("codigo_sindicato"),
            settings.api_motorista_sindicato_codigo,
        ]

        for candidato in candidatos:
            if candidato is None:
                continue
            txt = str(candidato).strip()
            if txt:
                return txt
        return ""

    @staticmethod
    def _empregador_padrao_por_codigo(codigo_empresa: str) -> dict[str, Any] | None:
        codigo = str(codigo_empresa or "").strip()
        codigo_regra = str(settings.api_empregador_codigo or "").strip()
        if not codigo or not codigo_regra or codigo != codigo_regra:
            return None

        cnpj = "".join(ch for ch in str(settings.api_empregador_cnpj or "") if ch.isdigit())
        nome = str(settings.api_empregador_nome or "").strip()
        cidade = str(settings.api_empregador_cidade or "").strip()
        uf = str(settings.api_empregador_uf or "").strip().upper()
        if not (cnpj and nome and cidade and uf):
            return None

        return {
            "codigo": codigo_regra,
            "nome": nome,
            "cnpj": cnpj,
            "endereco": {
                "cidade": cidade,
                "uf": uf,
            },
        }

    @staticmethod
    def _validar_payload_afastamento(payload: dict[str, Any]) -> None:
        cpf = str(payload.get("cpf") or "").strip()
        descricao = str(payload.get("descricao") or "").strip()
        datainicio = str(payload.get("datainicio") or "").strip()

        if not cpf:
            raise ValueError("Payload de afastamento sem CPF.")
        if not descricao:
            raise ValueError("Payload de afastamento sem descricao.")
        if not datainicio:
            raise ValueError("Payload de afastamento sem datainicio.")
        ApiDispatchService._validar_pessoa_juridica_obrigatoria(
            payload.get("empregador"),
            "empregador",
        )

    @staticmethod
    def _validar_payload_motorista(payload: dict[str, Any]) -> None:
        nome = str(payload.get("nome") or "").strip()
        cpf = str(payload.get("cpf") or "").strip()
        dataadmissao = str(payload.get("dataadmissao") or "").strip()
        endereco = payload.get("endereco")

        if not nome:
            raise ValueError("Payload de motorista sem nome.")
        if not cpf:
            raise ValueError("Payload de motorista sem CPF.")
        if not dataadmissao:
            raise ValueError("Payload de motorista sem dataadmissao.")
        if not isinstance(endereco, dict):
            raise ValueError("Payload de motorista sem endereco.")
        if not str(endereco.get("cidade") or "").strip() or not str(endereco.get("uf") or "").strip():
            raise ValueError("Endereco do motorista sem cidade/UF.")
        ApiDispatchService._validar_pessoa_juridica_obrigatoria(
            payload.get("empregador"),
            "empregador",
        )
        ApiDispatchService._validar_pessoa_juridica_obrigatoria(
            payload.get("sindicato"),
            "sindicato",
        )

    @staticmethod
    def _validar_pessoa_juridica_obrigatoria(pessoa: Any, campo: str) -> None:
        if not isinstance(pessoa, dict):
            raise ValueError(f"Payload sem {campo}.")

        nome = str(pessoa.get("nome") or "").strip()
        cnpj = "".join(ch for ch in str(pessoa.get("cnpj") or "") if ch.isdigit())
        if not nome:
            raise ValueError(f"{campo}.nome obrigatorio nao informado.")
        if not cnpj:
            raise ValueError(f"{campo}.cnpj obrigatorio nao informado.")

        endereco = pessoa.get("endereco")
        if not isinstance(endereco, dict):
            raise ValueError(f"{campo}.endereco obrigatorio nao informado.")
        cidade = str(endereco.get("cidade") or "").strip()
        uf = str(endereco.get("uf") or "").strip().upper()
        if not cidade or not uf:
            raise ValueError(f"{campo}.endereco sem cidade/UF.")

    @staticmethod
    def _resposta_indica_sucesso(response: ApiResponse) -> bool:
        if response.status_code < 200 or response.status_code >= 300:
            return False

        data = response.json_data
        if not isinstance(data, dict):
            return True

        retorno_id = data.get("id")
        if retorno_id is None:
            return True
        try:
            return int(retorno_id) == 0
        except Exception:
            return str(retorno_id).strip() == "0"

    @staticmethod
    def _mensagem_erro_resposta(response: ApiResponse) -> str:
        data = response.json_data
        if isinstance(data, dict):
            mensagem = str(data.get("mensagem") or "").strip()
            if mensagem:
                return mensagem
            retorno_id = data.get("id")
            if retorno_id is not None:
                return f"Retorno API id={retorno_id} sem mensagem."

        if response.text:
            return response.text
        return f"HTTP {response.status_code} sem corpo de resposta."

    def _resumo_resposta(self, response: ApiResponse) -> str | None:
        data = response.json_data
        if isinstance(data, dict):
            resumo = str(data.get("mensagem") or "").strip()
            if resumo:
                return self._limitar_texto(resumo)
        if response.text:
            return self._limitar_texto(response.text)
        return None

    @staticmethod
    def _limitar_texto(value: str | None, size: int = 1000) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        return text[:size]

    @staticmethod
    def _sindicato_padrao() -> dict[str, Any] | None:
        codigo = str(settings.api_motorista_sindicato_codigo or "").strip()
        nome = str(settings.api_motorista_sindicato_nome or "").strip()
        cnpj = "".join(ch for ch in str(settings.api_motorista_sindicato_cnpj or "") if ch.isdigit())
        cidade = str(settings.api_motorista_sindicato_cidade or "").strip()
        uf = str(settings.api_motorista_sindicato_uf or "").strip().upper()

        if not (nome and cnpj and cidade and uf):
            return None

        result = {
            "nome": nome,
            "cnpj": cnpj,
            "endereco": {
                "cidade": cidade,
                "uf": uf,
            },
        }
        if codigo:
            result["codigo"] = codigo
        return result

    @staticmethod
    def _extrair_colunas_origem(regras: list[dict[str, Any]]) -> list[str]:
        colunas: list[str] = []
        seen: set[str] = set()
        for regra in regras:
            origem = str(regra.get("origem") or "").strip()
            if not origem:
                continue

            origem_lower = origem.lower()
            coluna_nome = ""
            if origem_lower.startswith("colunas."):
                coluna_nome = origem.split(".", 1)[1].strip()
            elif origem_lower.startswith("coluna:"):
                coluna_nome = origem.split(":", 1)[1].strip()

            if not coluna_nome:
                continue

            chave = "".join(ch for ch in coluna_nome.lower() if ch.isalnum())
            if not chave or chave in seen:
                continue
            seen.add(chave)
            colunas.append(coluna_nome)

        return colunas

    @staticmethod
    def _montar_origem_de_para(
        payload: dict[str, Any],
        *,
        evento: dict[str, Any],
        colunas: dict[str, Any],
    ) -> dict[str, Any]:
        origem = dict(payload)
        origem["payload"] = dict(payload)
        origem["evento"] = dict(evento or {})
        origem["colunas"] = dict(colunas or {})
        return origem

    @staticmethod
    def _normalizar_de_para(raw_rules: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_rules, list):
            return []

        result: list[dict[str, Any]] = []
        for idx, raw in enumerate(raw_rules, start=1):
            if not isinstance(raw, dict):
                continue

            origem = str(raw.get("origem") or raw.get("source") or raw.get("from") or "").strip()
            destino = str(raw.get("destino") or raw.get("target") or raw.get("to") or "").strip()
            if not destino:
                continue

            regra: dict[str, Any] = {
                "nome": str(raw.get("nome") or f"regra_{idx}").strip(),
                "origem": origem,
                "destino": destino,
                "obrigatorio": bool(raw.get("obrigatorio", raw.get("required", False))),
                "ativo": bool(raw.get("ativo", raw.get("enabled", True))),
                "tem_padrao": ("padrao" in raw) or ("default" in raw),
                "padrao": raw.get("padrao", raw.get("default")),
                "transformacao": str(raw.get("transformacao") or raw.get("transform") or "").strip().lower(),
            }
            result.append(regra)

        return result

    def _aplicar_de_para(self, payload_origem: dict[str, Any], *, contexto: str) -> dict[str, Any]:
        payload_destino: dict[str, Any] = {}

        for regra in self.payload_mapping:
            if not bool(regra.get("ativo", True)):
                continue

            origem = str(regra.get("origem") or "").strip()
            destino = str(regra.get("destino") or "").strip()
            nome = str(regra.get("nome") or destino or origem or "campo").strip()
            obrigatorio = bool(regra.get("obrigatorio", False))
            tem_padrao = bool(regra.get("tem_padrao", False))
            padrao = regra.get("padrao")
            transformacao = str(regra.get("transformacao") or "").strip().lower()

            valor = self._obter_valor_por_caminho(payload_origem, origem) if origem else None
            if self._valor_vazio(valor) and tem_padrao:
                valor = padrao

            if self._valor_vazio(valor):
                if obrigatorio:
                    raise ValueError(
                        f"Campo obrigatorio ausente no de-para ({contexto}): '{nome}' "
                        f"(origem='{origem}', destino='{destino}')"
                    )
                continue

            valor = self._aplicar_transformacao(valor, transformacao)
            self._definir_valor_por_caminho(payload_destino, destino, valor)

        if not payload_destino:
            raise ValueError(f"De-para de {contexto} gerou payload vazio.")

        return payload_destino

    @staticmethod
    def _obter_valor_por_caminho(payload: Any, caminho: str) -> Any:
        if not caminho:
            return None

        atual: Any = payload
        for token in caminho.split("."):
            parte = token.strip()
            if not parte:
                continue
            if isinstance(atual, dict):
                atual = atual.get(parte)
                continue
            if isinstance(atual, list) and parte.isdigit():
                idx = int(parte)
                if 0 <= idx < len(atual):
                    atual = atual[idx]
                    continue
            return None

        return atual

    @staticmethod
    def _definir_valor_por_caminho(payload: dict[str, Any], caminho: str, valor: Any) -> None:
        partes = [p.strip() for p in caminho.split(".") if p.strip()]
        if not partes:
            return

        atual: dict[str, Any] = payload
        for parte in partes[:-1]:
            prox = atual.get(parte)
            if not isinstance(prox, dict):
                prox = {}
                atual[parte] = prox
            atual = prox
        atual[partes[-1]] = valor

    @staticmethod
    def _valor_vazio(valor: Any) -> bool:
        if valor is None:
            return True
        if isinstance(valor, str):
            return not valor.strip()
        return False

    @staticmethod
    def _aplicar_transformacao(valor: Any, transformacao: str) -> Any:
        if not transformacao:
            return valor

        name = transformacao.strip().lower()
        if name in {"str", "string", "texto"}:
            return str(valor)
        if name in {"upper", "maiusculo"}:
            return str(valor).upper()
        if name in {"lower", "minusculo"}:
            return str(valor).lower()
        if name in {"int", "inteiro"}:
            return int(float(str(valor).replace(",", ".")))
        if name in {"float", "decimal", "numero"}:
            return float(str(valor).replace(",", "."))
        if name in {"bool", "booleano"}:
            text = str(valor).strip().lower()
            if text in {"1", "true", "sim", "s", "y", "yes"}:
                return True
            if text in {"0", "false", "nao", "n", "no"}:
                return False
            return bool(valor)
        if name in {"cpf_digits", "cpf_digitos", "digits"}:
            return "".join(ch for ch in str(valor) if ch.isdigit())
        if name in {"date_yyyy_mm_dd", "data_yyyy_mm_dd", "yyyy_mm_dd"}:
            if isinstance(valor, datetime):
                return valor.strftime("%Y-%m-%d")
            txt = str(valor).strip()
            if len(txt) >= 10 and txt[4:5] == "-" and txt[7:8] == "-":
                return txt[:10]
            if len(txt) >= 10 and txt[2:3] == "/" and txt[5:6] == "/":
                dd, mm, yyyy = txt[:10].split("/")
                return f"{yyyy}-{mm}-{dd}"
            return txt

        return valor
