from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable

from sqlalchemy.engine import Engine

from Consultas_dbo.afastamentos.afastamentos import RepositorioAfastamentos
from Consultas_dbo.cadastrei.afastamento import RepositorioAfastamento
from Ferramentas.montar_payload_afastamentos import montar_payload_afastamentos


@dataclass
class ResultadoCicloAfastamentos:
    registros_origem: int = 0
    payloads_validos: int = 0
    eventos_gerados: int = 0
    eventos_inseridos: int = 0
    cursor_reiniciado: bool = False


class AfastamentoSyncService:
    def __init__(
        self,
        *,
        engine_origem: Engine,
        engine_destino: Engine,
        database_origem: str,
        schema_origem: str,
        schema_destino: str = "dbo",
        tabela_destino: str = "Afastamento",
        batch_size: int = 500,
        data_inicio: date | datetime | str | None = None,
    ) -> None:
        self.database_origem = (database_origem or "").strip()
        self.schema_origem = (schema_origem or "").strip()
        self.batch_size = max(1, int(batch_size))
        self.data_inicio = self._coerce_data_inicio(data_inicio)

        self.repo_origem = RepositorioAfastamentos(
            engine_origem,
            schema_origem=self.schema_origem,
        )
        self.repo_destino = RepositorioAfastamento(
            engine_destino,
            schema=schema_destino,
            table_name=tabela_destino,
        )

    def resetar_estado_sync(self) -> None:
        self.repo_destino.garantir_estruturas_auxiliares()
        self.repo_destino.resetar_estado_sync(self.database_origem)

    def executar_ciclo(self) -> ResultadoCicloAfastamentos:
        resultado = ResultadoCicloAfastamentos()

        self.repo_destino.garantir_estruturas_auxiliares()
        cursor = self.repo_destino.carregar_cursor(self.database_origem)

        rows = self.repo_origem.buscar_dados_afastamentos_por_cursor(
            limit=self.batch_size,
            c_numemp=cursor["NumEmp"],
            c_tipcol=cursor["TipCol"],
            c_numcad=cursor["NumCad"],
            c_datafa=cursor["DataFa"],
            c_horafa=cursor["HoraFa"],
            c_seqreg=cursor["SeqReg"],
            data_inicio=self.data_inicio,
        )

        if not rows and not self._cursor_em_inicio(cursor):
            cursor_inicial = self._cursor_inicial()
            self.repo_destino.salvar_cursor(
                self.database_origem,
                numemp=cursor_inicial["NumEmp"],
                tipcol=cursor_inicial["TipCol"],
                numcad=cursor_inicial["NumCad"],
                datafa=cursor_inicial["DataFa"],
                horafa=cursor_inicial["HoraFa"],
                seqreg=cursor_inicial["SeqReg"],
            )
            resultado.cursor_reiniciado = True
            return resultado

        if not rows:
            return resultado

        resultado.registros_origem = len(rows)

        payloads = montar_payload_afastamentos(rows)
        resultado.payloads_validos = len(payloads)

        payload_por_key = {
            self._key_from_payload(item): item
            for item in payloads
            if self._key_from_payload(item) is not None
        }

        chaves_estado = []
        for row in rows:
            key = self._state_key_from_row(row)
            if key is not None:
                chaves_estado.append(key)

        hashes_anteriores = self.repo_destino.carregar_hashes_por_chaves(
            self.database_origem,
            chaves_estado,
        )

        eventos: list[dict[str, Any]] = []
        hashes_novos: list[dict[str, Any]] = []

        for row in rows:
            key = self._state_key_from_row(row)
            if key is None:
                continue

            payload = payload_por_key.get(key)
            if not payload:
                continue

            payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            hash_payload = hashlib.sha256(payload_json.encode("utf-8")).digest()
            hash_anterior = hashes_anteriores.get(key)
            if hash_anterior == hash_payload:
                continue

            numempresa, tipocolaborador, numorigem, dataafastamento, situacao = key
            operacao = "I" if hash_anterior is None else "U"
            descricao_situacao = str(row.get("dessit") or "").strip()
            eventos.append(
                {
                    "numempresa": numempresa,
                    "tipocolaborador": tipocolaborador,
                    "numorigem": numorigem,
                    "dataafastamento": dataafastamento,
                    "horafa": self._to_int_or_none(row.get("horafa")),
                    "datter": self._to_date_or_none(row.get("datter")),
                    "horter": self._to_int_or_none(row.get("horter")),
                    "situacao": situacao,
                    "descricao": str(row.get("obsafa") or descricao_situacao or row.get("sitafa") or "Afastamento"),
                    "descricao_situacao": descricao_situacao or None,
                    "operacao": operacao,
                    "evento_tipo": "AFASTAMENTO_UPSERT",
                    "versao_payload": "v1",
                    "hash_payload": hash_payload,
                    "payload_json": payload_json,
                    "status": "PENDENTE",
                    "tentativas": 0,
                    "origem_tabela": "R038AFA",
                    "origem_sistema": "Vetorh",
                }
            )
            hashes_novos.append(
                {
                    "numempresa": numempresa,
                    "tipocolaborador": tipocolaborador,
                    "numorigem": numorigem,
                    "dataafastamento": dataafastamento,
                    "situacao": situacao,
                    "hash_payload": hash_payload,
                }
            )

        resultado.eventos_gerados = len(eventos)

        if eventos:
            resultado.eventos_inseridos = self.repo_destino.inserir_eventos(eventos)
            self.repo_destino.salvar_hashes_por_chaves(self.database_origem, hashes_novos)

        ultimo = rows[-1]
        self.repo_destino.salvar_cursor(
            self.database_origem,
            numemp=int(ultimo["numemp"]),
            tipcol=int(ultimo["tipcol"]),
            numcad=int(ultimo["numcad"]),
            datafa=ultimo["datafa"],
            horafa=self._to_int_or_none(ultimo.get("horafa")) or 0,
            seqreg=self._to_int_or_none(ultimo.get("seqreg")) or 0,
        )
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
                        f"Lidos={resultado.registros_origem} "
                        f"Payload={resultado.payloads_validos} "
                        f"Eventos={resultado.eventos_gerados} "
                        f"Inseridos={resultado.eventos_inseridos} "
                        f"ResetCursor={resultado.cursor_reiniciado}"
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

    @staticmethod
    def _to_int_or_none(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _coerce_data_inicio(value: date | datetime | str | None) -> date:
        if value is None:
            return date.today()
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        text = str(value).strip()
        if not text:
            return date.today()
        try:
            return datetime.fromisoformat(text[:10]).date()
        except Exception as exc:
            raise ValueError(
                f"data_inicio invalida: {value!r}. Use o formato YYYY-MM-DD."
            ) from exc

    @staticmethod
    def _to_date_or_none(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text[:10]).date()
        except Exception:
            return None

    def _state_key_from_row(self, row: dict[str, Any]) -> tuple[int, int, int, date, int] | None:
        dataafastamento = self._to_date_or_none(row.get("datafa"))
        if dataafastamento is None:
            return None
        try:
            return (
                int(row.get("numemp")),
                int(row.get("tipcol")),
                int(row.get("numcad")),
                dataafastamento,
                int(row.get("sitafa")),
            )
        except Exception:
            return None

    def _key_from_payload(self, payload: dict[str, Any]) -> tuple[int, int, int, date, int] | None:
        dataafastamento = self._to_date_or_none(payload.get("dataafastamento"))
        if dataafastamento is None:
            return None
        try:
            return (
                int(payload.get("numerodaempresa")),
                int(payload.get("tipodecolaborador")),
                int(payload.get("numerodeorigemdocolaborador")),
                dataafastamento,
                int(payload.get("situacao")),
            )
        except Exception:
            return None

    @staticmethod
    def _cursor_inicial() -> dict[str, Any]:
        return {
            "NumEmp": 0,
            "TipCol": 0,
            "NumCad": 0,
            "DataFa": datetime(1900, 1, 1),
            "HoraFa": -1,
            "SeqReg": -1,
        }

    def _cursor_em_inicio(self, cursor: dict[str, Any]) -> bool:
        base = self._cursor_inicial()
        return (
            int(cursor.get("NumEmp", 0)) == base["NumEmp"]
            and int(cursor.get("TipCol", 0)) == base["TipCol"]
            and int(cursor.get("NumCad", 0)) == base["NumCad"]
            and self._to_date_or_none(cursor.get("DataFa")) == base["DataFa"].date()
            and int(cursor.get("HoraFa", -1)) == base["HoraFa"]
            and int(cursor.get("SeqReg", -1)) == base["SeqReg"]
        )
