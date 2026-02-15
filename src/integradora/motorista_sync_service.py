from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from sqlalchemy.engine import Engine

from Consultas_dbo.cadastrei.motorista_cadastro import RepositorioMotoristaCadastro
from Consultas_dbo.cadastro_motoristas.cadastro_motoristas import RepositorioCadastroMotoristas
from Ferramentas.montar_payload_motoristas import montar_payload_motoristas


@dataclass
class ResultadoCicloMotoristas:
    alterados_fun: int = 0
    alterados_cpl: int = 0
    numcads_processados: int = 0
    registros_origem: int = 0
    payloads_validos: int = 0
    eventos_gerados: int = 0
    eventos_inseridos: int = 0


class MotoristaSyncService:
    def __init__(
        self,
        *,
        engine_origem: Engine,
        engine_destino: Engine,
        database_origem: str,
        schema_origem: str,
        schema_destino: str = "dbo",
        tabela_destino: str = "MotoristaCadastro",
        batch_size: int = 500,
    ) -> None:
        self.database_origem = (database_origem or "").strip()
        self.schema_origem = (schema_origem or "").strip()
        self.batch_size = max(1, int(batch_size))

        self.repo_origem = RepositorioCadastroMotoristas(
            engine_origem,
            schema_origem=self.schema_origem,
        )
        self.repo_destino = RepositorioMotoristaCadastro(
            engine_destino,
            schema=schema_destino,
            table_name=tabela_destino,
        )

    def resetar_estado_sync(self) -> None:
        self.repo_destino.garantir_estruturas_auxiliares()
        self.repo_destino.resetar_estado_sync(self.database_origem)

    def executar_ciclo(self) -> ResultadoCicloMotoristas:
        resultado = ResultadoCicloMotoristas()

        self.repo_destino.garantir_estruturas_auxiliares()

        checkpoint_fun = self.repo_destino.carregar_checkpoint(self.database_origem, "R034FUN")
        checkpoint_cpl = self.repo_destino.carregar_checkpoint(self.database_origem, "R034CPL")

        alterados_fun = self.repo_origem.buscar_numcads_alterados(
            tabela_origem="R034FUN",
            limite=self.batch_size,
            ultima_alteracao=checkpoint_fun[0],
            ultimo_numcad=checkpoint_fun[1],
        )
        alterados_cpl = self.repo_origem.buscar_numcads_alterados(
            tabela_origem="R034CPL",
            limite=self.batch_size,
            ultima_alteracao=checkpoint_cpl[0],
            ultimo_numcad=checkpoint_cpl[1],
        )

        resultado.alterados_fun = len(alterados_fun)
        resultado.alterados_cpl = len(alterados_cpl)

        origem_por_numcad: dict[int, set[str]] = {}
        for row in alterados_fun:
            origem_por_numcad.setdefault(int(row["numcad"]), set()).add("R034FUN")
        for row in alterados_cpl:
            origem_por_numcad.setdefault(int(row["numcad"]), set()).add("R034CPL")

        numcads = sorted(origem_por_numcad.keys())
        resultado.numcads_processados = len(numcads)
        if not numcads:
            self._salvar_checkpoints(alterados_fun, alterados_cpl)
            return resultado

        registros = self.repo_origem.buscar_dados_cadastro_motoristas_por_numcads(numcads)
        resultado.registros_origem = len(registros)
        registros_por_numcad: dict[int, dict[str, Any]] = {}
        for row in registros:
            try:
                key = int(row.get("numcad"))
            except Exception:
                continue
            registros_por_numcad[key] = row

        payload = montar_payload_motoristas(registros)
        resultado.payloads_validos = len(payload)

        payload_por_numcad: dict[int, dict[str, Any]] = {}
        for item in payload:
            matricula = item.get("matricula")
            try:
                numcad = int(str(matricula).strip())
            except Exception:
                continue
            payload_por_numcad[numcad] = item

        hashes_anteriores = self.repo_destino.carregar_hashes_por_origem(self.database_origem, numcads)

        eventos: list[dict[str, Any]] = []
        hashes_novos: list[dict[str, Any]] = []

        for numcad in numcads:
            item = payload_por_numcad.get(numcad)
            if not item:
                continue

            origem = registros_por_numcad.get(numcad, {})
            payload_json = json.dumps(item, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            hash_payload = hashlib.sha256(payload_json.encode("utf-8")).digest()
            hash_anterior = hashes_anteriores.get(numcad)
            fontes = origem_por_numcad.get(numcad, set())
            houve_update_fun = "R034FUN" in fontes

            # Se houve update em R034FUN, sempre gera evento, mesmo quando o payload
            # final volta a ser igual ao historico anterior (cenario de reversao).
            if hash_anterior == hash_payload and not houve_update_fun:
                continue

            operacao = "I" if hash_anterior is None else "U"
            origem_tabela = "/".join(sorted(fontes or {"R034FUN", "R034CPL"}))

            eventos.append(
                {
                    "id_de_origem": numcad,
                    "operacao": operacao,
                    "evento_tipo": "MOTORISTA_UPSERT",
                    "versao_payload": "v1",
                    "hash_payload": hash_payload,
                    "payload_json": payload_json,
                    "status": "PENDENTE",
                    "tentativas": 0,
                    "origem_tabela": origem_tabela,
                    "origem_sistema": "Vetorh",
                    "cpf": item.get("cpf"),
                    "matricula": item.get("matricula"),
                    "nome": item.get("nome"),
                    "centro_de_custo": origem.get("codccu"),
                    "tipo_de_colaborador": origem.get("tipcol"),
                    "situacao": origem.get("sitafa"),
                    "nome_do_motorista": origem.get("nomfun") or item.get("nome"),
                    "numero_do_cpf": origem.get("numcpf") or item.get("cpf"),
                    "data_do_nascimento": origem.get("datnas"),
                    "sexo": origem.get("tipsex"),
                    "estado_de_residencia": origem.get("estado_residencia"),
                    "pais_do_cadastro": origem.get("pais"),
                    "naturalidade": origem.get("naturalidade"),
                    "pais": origem.get("pais"),
                    "orgao_expedidor_do_rg": origem.get("orgao_expedidor_rg"),
                    "data_de_emissao_da_cnh": origem.get("datcnh"),
                    "data_de_vencimento_da_cnh": origem.get("vencnh"),
                    "numero_do_rg": origem.get("numero_rg"),
                    "numero_da_cnh": origem.get("numcnh"),
                    "categoria_da_cnh": origem.get("catcnh"),
                    "numero_do_registro_da_cnh": origem.get("pricnh"),
                    "estado_civil": origem.get("estado_civil"),
                    "nome_da_mae": origem.get("nome_mae"),
                    "cidade": origem.get("cidade"),
                    "logradouro": origem.get("logradouro"),
                    "bairro": origem.get("bairro"),
                    "numero_da_residencia": origem.get("numero"),
                    "ddd": origem.get("dddtel"),
                    "numero_de_telefone": origem.get("numtel"),
                    "numemp": origem.get("numemp"),
                }
            )
            hashes_novos.append({"id_de_origem": numcad, "hash_payload": hash_payload})

        resultado.eventos_gerados = len(eventos)
        if not eventos:
            self._salvar_checkpoints(alterados_fun, alterados_cpl)
            return resultado

        inseridos = self.repo_destino.inserir_eventos(eventos)
        resultado.eventos_inseridos = inseridos

        self.repo_destino.salvar_hashes_por_origem(self.database_origem, hashes_novos)
        self._salvar_checkpoints(alterados_fun, alterados_cpl)
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
                        f"FUN={resultado.alterados_fun} "
                        f"CPL={resultado.alterados_cpl} "
                        f"NumCad={resultado.numcads_processados} "
                        f"Payload={resultado.payloads_validos} "
                        f"Eventos={resultado.eventos_gerados} "
                        f"Inseridos={resultado.eventos_inseridos}"
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

    def _salvar_checkpoints(
        self,
        alterados_fun: list[dict[str, Any]],
        alterados_cpl: list[dict[str, Any]],
    ) -> None:
        if alterados_fun:
            ultimo = alterados_fun[-1]
            self.repo_destino.salvar_checkpoint(
                self.database_origem,
                "R034FUN",
                ultima_alteracao=ultimo["change_dt"],
                ultimo_numcad=int(ultimo["numcad"]),
            )

        if alterados_cpl:
            ultimo = alterados_cpl[-1]
            self.repo_destino.salvar_checkpoint(
                self.database_origem,
                "R034CPL",
                ultima_alteracao=ultimo["change_dt"],
                ultimo_numcad=int(ultimo["numcad"]),
            )
