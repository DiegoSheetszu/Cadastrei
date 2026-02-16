from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

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
    ) -> None:
        self.batch_size_motoristas = max(1, int(batch_size_motoristas))
        self.batch_size_afastamentos = max(1, int(batch_size_afastamentos))
        self.max_tentativas = max(1, int(max_tentativas))
        self.lock_timeout_minutes = max(1, int(lock_timeout_minutes))
        self.retry_base_seconds = max(1, int(retry_base_seconds))
        self.retry_max_seconds = max(self.retry_base_seconds, int(retry_max_seconds))
        self.endpoint_motorista = endpoint_motorista or settings.api_motorista_endpoint
        self.endpoint_afastamento = endpoint_afastamento or settings.api_afastamento_endpoint

        self.repo = RepositorioFilaIntegracaoApi(
            engine_destino,
            schema=schema_destino,
            tabela_motorista=tabela_motorista,
            tabela_afastamento=tabela_afastamento,
        )
        self.api_client = AtsApiClient(timeout_seconds=api_timeout_seconds)

    def close(self) -> None:
        self.api_client.close()

    def executar_ciclo(self) -> ResultadoCicloApi:
        resultado = ResultadoCicloApi()

        locks = self.repo.liberar_locks_expirados(
            lock_timeout_minutes=self.lock_timeout_minutes
        )
        resultado.locks_liberados_motoristas = int(locks.get("motoristas") or 0)
        resultado.locks_liberados_afastamentos = int(locks.get("afastamentos") or 0)

        lock_id_motoristas = f"M-{uuid.uuid4().hex}"
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

        lock_id_afastamentos = f"A-{uuid.uuid4().hex}"
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
            payload = self._enriquecer_payload_motorista(payload)
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
        nome = str(settings.api_motorista_sindicato_nome or "").strip()
        cnpj = str(settings.api_motorista_sindicato_cnpj or "").strip()
        cidade = str(settings.api_motorista_sindicato_cidade or "").strip()
        uf = str(settings.api_motorista_sindicato_uf or "").strip().upper()

        if not (nome and cnpj and cidade and uf):
            return None

        return {
            "nome": nome,
            "cnpj": cnpj,
            "endereco": {
                "cidade": cidade,
                "uf": uf,
            },
        }
