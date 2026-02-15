from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_COLUNAS_EVENTO_OBRIGATORIAS = (
    "NumeroDaEmpresa",
    "TipoDeColaborador",
    "NumeroDeOrigemDoColaborador",
    "DataDoAfastamento",
    "Situacao",
    "Operacao",
    "EventoTipo",
    "VersaoPayload",
    "HashPayload",
    "PayloadJson",
    "Status",
    "Tentativas",
    "OrigemTabela",
    "CriadoEm",
    "AtualizadoEm",
)


def _safe_identifier(value: str, label: str) -> str:
    normalized = (value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"{label} invalido: {value!r}")
    return normalized


def _normalize_key(value: str) -> str:
    return "".join(ch for ch in str(value).lower() if ch.isalnum())


class RepositorioAfastamento:
    def __init__(self, engine: Engine, schema: str = "dbo", table_name: str = "Afastamento"):
        self.engine = engine
        self.schema = _safe_identifier(schema, "Schema")
        self.table_name = _safe_identifier(table_name, "Tabela")
        self._cache_colunas_tabela: dict[str, str] | None = None

    def garantir_estruturas_auxiliares(self) -> None:
        sql_estado = text(
            f"""
            IF OBJECT_ID(N'[{self.schema}].[AfastamentoSyncEstado]', 'U') IS NULL
            BEGIN
                CREATE TABLE [{self.schema}].[AfastamentoSyncEstado](
                    [DatabaseOrigem] SYSNAME NOT NULL,
                    [NumeroDaEmpresa] INT NOT NULL,
                    [TipoDeColaborador] SMALLINT NOT NULL,
                    [NumeroDeOrigemDoColaborador] INT NOT NULL,
                    [DataDoAfastamento] DATE NOT NULL,
                    [Situacao] INT NOT NULL,
                    [HashPayload] VARBINARY(32) NOT NULL,
                    [AtualizadoEm] DATETIME2(0) NOT NULL CONSTRAINT [DF_AfastamentoSyncEstado_AtualizadoEm] DEFAULT (SYSUTCDATETIME()),
                    CONSTRAINT [PK_AfastamentoSyncEstado] PRIMARY KEY (
                        [DatabaseOrigem],
                        [NumeroDaEmpresa],
                        [TipoDeColaborador],
                        [NumeroDeOrigemDoColaborador],
                        [DataDoAfastamento],
                        [Situacao]
                    )
                );
            END
            """
        )

        sql_cursor = text(
            f"""
            IF OBJECT_ID(N'[{self.schema}].[AfastamentoSyncCursor]', 'U') IS NULL
            BEGIN
                CREATE TABLE [{self.schema}].[AfastamentoSyncCursor](
                    [DatabaseOrigem] SYSNAME NOT NULL,
                    [NumEmp] INT NOT NULL,
                    [TipCol] SMALLINT NOT NULL,
                    [NumCad] INT NOT NULL,
                    [DataFa] DATETIME2(0) NOT NULL,
                    [HoraFa] INT NOT NULL,
                    [SeqReg] BIGINT NOT NULL,
                    [AtualizadoEm] DATETIME2(0) NOT NULL CONSTRAINT [DF_AfastamentoSyncCursor_AtualizadoEm] DEFAULT (SYSUTCDATETIME()),
                    CONSTRAINT [PK_AfastamentoSyncCursor] PRIMARY KEY ([DatabaseOrigem])
                );
            END
            """
        )

        sql_coluna_descricao_situacao = text(
            f"""
            IF OBJECT_ID(N'[{self.schema}].[{self.table_name}]', 'U') IS NOT NULL
            AND COL_LENGTH(N'{self.schema}.{self.table_name}', 'DescricaoDaSituacao') IS NULL
            BEGIN
                ALTER TABLE [{self.schema}].[{self.table_name}]
                ADD [DescricaoDaSituacao] NVARCHAR(200) NULL;
            END
            """
        )

        with self.engine.begin() as conn:
            conn.execute(sql_estado)
            conn.execute(sql_cursor)
            conn.execute(sql_coluna_descricao_situacao)

    def carregar_cursor(self, database_origem: str) -> dict[str, Any]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT [NumEmp], [TipCol], [NumCad], [DataFa], [HoraFa], [SeqReg]
                    FROM [{self.schema}].[AfastamentoSyncCursor]
                    WHERE [DatabaseOrigem] = :database_origem
                    """
                ),
                {"database_origem": database_origem},
            ).mappings().first()

        if row:
            return dict(row)

        return {
            "NumEmp": 0,
            "TipCol": 0,
            "NumCad": 0,
            "DataFa": datetime(1900, 1, 1),
            "HoraFa": -1,
            "SeqReg": -1,
        }

    def salvar_cursor(
        self,
        database_origem: str,
        *,
        numemp: int,
        tipcol: int,
        numcad: int,
        datafa: datetime,
        horafa: int,
        seqreg: int,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    MERGE [{self.schema}].[AfastamentoSyncCursor] AS target
                    USING (
                        SELECT
                            :database_origem AS [DatabaseOrigem],
                            :numemp AS [NumEmp],
                            :tipcol AS [TipCol],
                            :numcad AS [NumCad],
                            :datafa AS [DataFa],
                            :horafa AS [HoraFa],
                            :seqreg AS [SeqReg]
                    ) AS source
                    ON target.[DatabaseOrigem] = source.[DatabaseOrigem]
                    WHEN MATCHED THEN
                        UPDATE SET
                            [NumEmp] = source.[NumEmp],
                            [TipCol] = source.[TipCol],
                            [NumCad] = source.[NumCad],
                            [DataFa] = source.[DataFa],
                            [HoraFa] = source.[HoraFa],
                            [SeqReg] = source.[SeqReg],
                            [AtualizadoEm] = SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN
                        INSERT ([DatabaseOrigem], [NumEmp], [TipCol], [NumCad], [DataFa], [HoraFa], [SeqReg], [AtualizadoEm])
                        VALUES (
                            source.[DatabaseOrigem],
                            source.[NumEmp],
                            source.[TipCol],
                            source.[NumCad],
                            source.[DataFa],
                            source.[HoraFa],
                            source.[SeqReg],
                            SYSUTCDATETIME()
                        );
                    """
                ),
                {
                    "database_origem": database_origem,
                    "numemp": int(numemp),
                    "tipcol": int(tipcol),
                    "numcad": int(numcad),
                    "datafa": datafa,
                    "horafa": int(horafa),
                    "seqreg": int(seqreg),
                },
            )

    def resetar_estado_sync(self, database_origem: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    DELETE FROM [{self.schema}].[AfastamentoSyncEstado]
                    WHERE [DatabaseOrigem] = :database_origem
                    """
                ),
                {"database_origem": database_origem},
            )
            conn.execute(
                text(
                    f"""
                    DELETE FROM [{self.schema}].[AfastamentoSyncCursor]
                    WHERE [DatabaseOrigem] = :database_origem
                    """
                ),
                {"database_origem": database_origem},
            )

    def carregar_hashes_por_chaves(
        self,
        database_origem: str,
        chaves: list[tuple[int, int, int, date, int]],
    ) -> dict[tuple[int, int, int, date, int], bytes]:
        keys = list(
            {
                (
                    int(numemp),
                    int(tipcol),
                    int(numcad),
                    datafa,
                    int(situacao),
                )
                for numemp, tipcol, numcad, datafa, situacao in chaves
            }
        )
        if not keys:
            return {}

        result: dict[tuple[int, int, int, date, int], bytes] = {}
        chunk_size = 300  # 1 + (300 * 5) = 1501 parametros (abaixo do limite SQL Server 2100)

        with self.engine.connect() as conn:
            for start in range(0, len(keys), chunk_size):
                chunk = keys[start:start + chunk_size]
                params: dict[str, Any] = {"database_origem": database_origem}
                conditions = []

                for i, (numemp, tipcol, numcad, datafa, situacao) in enumerate(chunk):
                    conditions.append(
                        "("
                        f"[NumeroDaEmpresa] = :e{i} "
                        f"AND [TipoDeColaborador] = :t{i} "
                        f"AND [NumeroDeOrigemDoColaborador] = :n{i} "
                        f"AND [DataDoAfastamento] = :d{i} "
                        f"AND [Situacao] = :s{i}"
                        ")"
                    )
                    params[f"e{i}"] = numemp
                    params[f"t{i}"] = tipcol
                    params[f"n{i}"] = numcad
                    params[f"d{i}"] = datafa
                    params[f"s{i}"] = situacao

                sql = text(
                    f"""
                    SELECT
                        [NumeroDaEmpresa],
                        [TipoDeColaborador],
                        [NumeroDeOrigemDoColaborador],
                        [DataDoAfastamento],
                        [Situacao],
                        [HashPayload]
                    FROM [{self.schema}].[AfastamentoSyncEstado]
                    WHERE [DatabaseOrigem] = :database_origem
                    AND ({' OR '.join(conditions)})
                    """
                )
                rows = conn.execute(sql, params).mappings().all()

                for row in rows:
                    key = (
                        int(row["NumeroDaEmpresa"]),
                        int(row["TipoDeColaborador"]),
                        int(row["NumeroDeOrigemDoColaborador"]),
                        row["DataDoAfastamento"],
                        int(row["Situacao"]),
                    )
                    result[key] = row["HashPayload"]

        return result

    def salvar_hashes_por_chaves(
        self,
        database_origem: str,
        hashes: list[dict[str, Any]],
    ) -> None:
        if not hashes:
            return

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    MERGE [{self.schema}].[AfastamentoSyncEstado] AS target
                    USING (
                        SELECT
                            :database_origem AS [DatabaseOrigem],
                            :numempresa AS [NumeroDaEmpresa],
                            :tipocolaborador AS [TipoDeColaborador],
                            :numorigem AS [NumeroDeOrigemDoColaborador],
                            :dataafastamento AS [DataDoAfastamento],
                            :situacao AS [Situacao],
                            :hash_payload AS [HashPayload]
                    ) AS source
                    ON target.[DatabaseOrigem] = source.[DatabaseOrigem]
                    AND target.[NumeroDaEmpresa] = source.[NumeroDaEmpresa]
                    AND target.[TipoDeColaborador] = source.[TipoDeColaborador]
                    AND target.[NumeroDeOrigemDoColaborador] = source.[NumeroDeOrigemDoColaborador]
                    AND target.[DataDoAfastamento] = source.[DataDoAfastamento]
                    AND target.[Situacao] = source.[Situacao]
                    WHEN MATCHED THEN
                        UPDATE SET
                            [HashPayload] = source.[HashPayload],
                            [AtualizadoEm] = SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN
                        INSERT (
                            [DatabaseOrigem],
                            [NumeroDaEmpresa],
                            [TipoDeColaborador],
                            [NumeroDeOrigemDoColaborador],
                            [DataDoAfastamento],
                            [Situacao],
                            [HashPayload],
                            [AtualizadoEm]
                        )
                        VALUES (
                            source.[DatabaseOrigem],
                            source.[NumeroDaEmpresa],
                            source.[TipoDeColaborador],
                            source.[NumeroDeOrigemDoColaborador],
                            source.[DataDoAfastamento],
                            source.[Situacao],
                            source.[HashPayload],
                            SYSUTCDATETIME()
                        );
                    """
                ),
                [
                    {
                        "database_origem": database_origem,
                        "numempresa": int(item["numempresa"]),
                        "tipocolaborador": int(item["tipocolaborador"]),
                        "numorigem": int(item["numorigem"]),
                        "dataafastamento": item["dataafastamento"],
                        "situacao": int(item["situacao"]),
                        "hash_payload": item["hash_payload"],
                    }
                    for item in hashes
                ],
            )

    def inserir_eventos(self, eventos: list[dict[str, Any]]) -> int:
        if not eventos:
            return 0

        colunas = self._carregar_colunas_tabela()
        self._validar_colunas_obrigatorias(colunas)

        mapping_colunas = self._resolver_colunas_para_insert(colunas)
        cols_insert = list(mapping_colunas.keys())

        col_numempresa = colunas[_normalize_key("NumeroDaEmpresa")]
        col_tipocol = colunas[_normalize_key("TipoDeColaborador")]
        col_numorigem = colunas[_normalize_key("NumeroDeOrigemDoColaborador")]
        col_datafa = colunas[_normalize_key("DataDoAfastamento")]
        col_situacao = colunas[_normalize_key("Situacao")]
        col_hash = colunas[_normalize_key("HashPayload")]
        col_status = colunas[_normalize_key("Status")]

        sql = text(
            f"""
            INSERT INTO [{self.schema}].[{self.table_name}] ({", ".join(f"[{c}]" for c in cols_insert)})
            SELECT {", ".join(f":{p}" for p in mapping_colunas.values())}
            WHERE NOT EXISTS (
                SELECT 1
                FROM [{self.schema}].[{self.table_name}] AS t
                WHERE t.[{col_numempresa}] = :check_numempresa
                AND t.[{col_tipocol}] = :check_tipocolaborador
                AND t.[{col_numorigem}] = :check_numorigem
                AND t.[{col_datafa}] = :check_dataafastamento
                AND t.[{col_situacao}] = :check_situacao
                AND t.[{col_hash}] = :check_hash_payload
                AND t.[{col_status}] IN ('PENDENTE', 'ERRO')
            )
            """
        )

        inseridos = 0
        with self.engine.begin() as conn:
            for evento in eventos:
                params = self._montar_params_evento(mapping_colunas, evento)
                try:
                    result = conn.execute(sql, params)
                    inseridos += int(result.rowcount or 0)
                except IntegrityError as exc:
                    if "UX_Afastamento_Idem" in str(exc):
                        continue
                    raise

        return inseridos

    def _carregar_colunas_tabela(self) -> dict[str, str]:
        if self._cache_colunas_tabela is not None:
            return self._cache_colunas_tabela

        with self.engine.connect() as conn:
            colunas = conn.execute(
                text(
                    """
                    SELECT c.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS AS c
                    WHERE c.TABLE_SCHEMA = :schema
                    AND c.TABLE_NAME = :tabela
                    """
                ),
                {"schema": self.schema, "tabela": self.table_name},
            ).scalars().all()

        if not colunas:
            raise ValueError(f"Tabela nao encontrada: [{self.schema}].[{self.table_name}]")

        self._cache_colunas_tabela = {_normalize_key(c): c for c in colunas}
        return self._cache_colunas_tabela

    @staticmethod
    def _validar_colunas_obrigatorias(colunas: dict[str, str]) -> None:
        faltantes = [
            nome
            for nome in _COLUNAS_EVENTO_OBRIGATORIAS
            if _normalize_key(nome) not in colunas
        ]
        if faltantes:
            raise ValueError(
                "Colunas obrigatorias nao encontradas em Afastamento: "
                + ", ".join(faltantes)
            )

    def _resolver_colunas_para_insert(self, colunas: dict[str, str]) -> dict[str, str]:
        mapping = {
            colunas[_normalize_key("NumeroDaEmpresa")]: "numempresa",
            colunas[_normalize_key("TipoDeColaborador")]: "tipocolaborador",
            colunas[_normalize_key("NumeroDeOrigemDoColaborador")]: "numorigem",
            colunas[_normalize_key("DataDoAfastamento")]: "dataafastamento",
            colunas[_normalize_key("Situacao")]: "situacao",
            colunas[_normalize_key("Operacao")]: "operacao",
            colunas[_normalize_key("EventoTipo")]: "evento_tipo",
            colunas[_normalize_key("VersaoPayload")]: "versao_payload",
            colunas[_normalize_key("HashPayload")]: "hash_payload",
            colunas[_normalize_key("PayloadJson")]: "payload_json",
            colunas[_normalize_key("Status")]: "status",
            colunas[_normalize_key("Tentativas")]: "tentativas",
            colunas[_normalize_key("OrigemTabela")]: "origem_tabela",
            colunas[_normalize_key("CriadoEm")]: "criado_em",
            colunas[_normalize_key("AtualizadoEm")]: "atualizado_em",
        }

        optional = {
            "HoraDoAfastamento": "horafa",
            "DataDoTermino": "datter",
            "HoraDoTermino": "horter",
            "Descricao": "descricao",
            "DescricaoDaSituacao": "descricao_situacao",
            "OrigemSistema": "origem_sistema",
            "UsuarioBanco": "usuario_banco",
            "ProximaTentativaEm": "proxima_tentativa_em",
            "UltimoErro": "ultimo_erro",
            "HttpStatus": "http_status",
            "RespostaResumo": "resposta_resumo",
            "LockId": "lock_id",
            "LockEm": "lock_em",
            "ProcessadoEm": "processado_em",
        }
        for coluna, param in optional.items():
            key = _normalize_key(coluna)
            if key in colunas:
                mapping[colunas[key]] = param

        return mapping

    @staticmethod
    def _montar_params_evento(mapping_colunas: dict[str, str], evento: dict[str, Any]) -> dict[str, Any]:
        agora = datetime.utcnow()
        base = {
            "numempresa": int(evento["numempresa"]),
            "tipocolaborador": int(evento["tipocolaborador"]),
            "numorigem": int(evento["numorigem"]),
            "dataafastamento": evento["dataafastamento"],
            "horafa": evento.get("horafa"),
            "datter": evento.get("datter"),
            "horter": evento.get("horter"),
            "situacao": int(evento["situacao"]),
            "descricao": evento.get("descricao"),
            "descricao_situacao": evento.get("descricao_situacao"),
            "operacao": evento["operacao"],
            "evento_tipo": evento.get("evento_tipo") or "AFASTAMENTO_UPSERT",
            "versao_payload": evento.get("versao_payload") or "v1",
            "hash_payload": evento["hash_payload"],
            "payload_json": evento["payload_json"],
            "status": evento.get("status") or "PENDENTE",
            "tentativas": int(evento.get("tentativas", 0)),
            "origem_tabela": evento.get("origem_tabela") or "R038AFA",
            "origem_sistema": evento.get("origem_sistema") or "Vetorh",
            "usuario_banco": evento.get("usuario_banco"),
            "proxima_tentativa_em": evento.get("proxima_tentativa_em"),
            "ultimo_erro": evento.get("ultimo_erro"),
            "http_status": evento.get("http_status"),
            "resposta_resumo": evento.get("resposta_resumo"),
            "lock_id": evento.get("lock_id"),
            "lock_em": evento.get("lock_em"),
            "processado_em": evento.get("processado_em"),
            "criado_em": evento.get("criado_em") or agora,
            "atualizado_em": evento.get("atualizado_em") or agora,
        }

        params = {
            param_name: base.get(param_name)
            for param_name in set(mapping_colunas.values())
        }
        params["check_numempresa"] = base["numempresa"]
        params["check_tipocolaborador"] = base["tipocolaborador"]
        params["check_numorigem"] = base["numorigem"]
        params["check_dataafastamento"] = base["dataafastamento"]
        params["check_situacao"] = base["situacao"]
        params["check_hash_payload"] = base["hash_payload"]
        return params
