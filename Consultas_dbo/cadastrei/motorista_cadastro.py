from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_COLUNAS_EVENTO_OBRIGATORIAS = (
    "IdDeOrigem",
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

_CAMPOS_ESPELHO_ALIAS = {
    "cpf": ("Cpf", "CPF"),
    "matricula": ("Matricula", "MATRICULA", "NumeroMatricula", "NumCad"),
    "nome": ("Nome", "NOME", "NomeMotorista"),
}

_COLUNAS_ESPELHO_DIRETAS = {
    "CentroDeCusto": "centro_de_custo",
    "TipoDeColaborador": "tipo_de_colaborador",
    "Situacao": "situacao",
    "NomeDoMotorista": "nome_do_motorista",
    "NumeroDoCPF": "numero_do_cpf",
    "DataDoNascimento": "data_do_nascimento",
    "Sexo": "sexo",
    "EstadoDeResidencia": "estado_de_residencia",
    "PaisDoCadastro": "pais_do_cadastro",
    "Naturalidade": "naturalidade",
    "Pais": "pais",
    "OrgaoExpedidorDoRG": "orgao_expedidor_do_rg",
    "DataDeEmissaoDaCNH": "data_de_emissao_da_cnh",
    "DataDeVencimentoDaCNH": "data_de_vencimento_da_cnh",
    "NumeroDoRG": "numero_do_rg",
    "NumeroDaCNH": "numero_da_cnh",
    "CategoriaDaCNH": "categoria_da_cnh",
    "NumeroDoRegistroDaCNH": "numero_do_registro_da_cnh",
    "EstadoCivil": "estado_civil",
    "NomeDaMae": "nome_da_mae",
    "Cidade": "cidade",
    "Logradouro": "logradouro",
    "Bairro": "bairro",
    "NumeroDaResidencia": "numero_da_residencia",
    "DDD": "ddd",
    "NumeroDeTelefone": "numero_de_telefone",
    "NumEmp": "numemp",
}


def _safe_identifier(value: str, label: str) -> str:
    normalized = (value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"{label} invalido: {value!r}")
    return normalized


def _normalize_key(value: str) -> str:
    return "".join(ch for ch in str(value).lower() if ch.isalnum())


class RepositorioMotoristaCadastro:
    def __init__(self, engine: Engine, schema: str = "dbo", table_name: str = "MotoristaCadastro"):
        self.engine = engine
        self.schema = _safe_identifier(schema, "Schema")
        self.table_name = _safe_identifier(table_name, "Tabela")
        self._cache_colunas_tabela: dict[str, str] | None = None

    def garantir_estruturas_auxiliares(self) -> None:
        sql_estado = text(
            f"""
            IF OBJECT_ID(N'[{self.schema}].[MotoristaSyncEstado]', 'U') IS NULL
            BEGIN
                CREATE TABLE [{self.schema}].[MotoristaSyncEstado](
                    [DatabaseOrigem] SYSNAME NOT NULL,
                    [IdDeOrigem] INT NOT NULL,
                    [HashPayload] VARBINARY(32) NOT NULL,
                    [AtualizadoEm] DATETIME2(0) NOT NULL CONSTRAINT [DF_MotoristaSyncEstado_AtualizadoEm] DEFAULT (SYSUTCDATETIME()),
                    CONSTRAINT [PK_MotoristaSyncEstado] PRIMARY KEY ([DatabaseOrigem], [IdDeOrigem])
                );
            END
            """
        )

        sql_checkpoint = text(
            f"""
            IF OBJECT_ID(N'[{self.schema}].[MotoristaSyncCheckpoint]', 'U') IS NULL
            BEGIN
                CREATE TABLE [{self.schema}].[MotoristaSyncCheckpoint](
                    [DatabaseOrigem] SYSNAME NOT NULL,
                    [TabelaOrigem] SYSNAME NOT NULL,
                    [UltimaAlteracao] DATETIME2(0) NOT NULL,
                    [UltimoNumCad] INT NOT NULL,
                    [AtualizadoEm] DATETIME2(0) NOT NULL CONSTRAINT [DF_MotoristaSyncCheckpoint_AtualizadoEm] DEFAULT (SYSUTCDATETIME()),
                    CONSTRAINT [PK_MotoristaSyncCheckpoint] PRIMARY KEY ([DatabaseOrigem], [TabelaOrigem])
                );
            END
            """
        )

        with self.engine.begin() as conn:
            conn.execute(sql_estado)
            conn.execute(sql_checkpoint)

    def carregar_checkpoint(self, database_origem: str, tabela_origem: str) -> tuple[datetime, int]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT [UltimaAlteracao], [UltimoNumCad]
                    FROM [{self.schema}].[MotoristaSyncCheckpoint]
                    WHERE [DatabaseOrigem] = :database_origem
                    AND [TabelaOrigem] = :tabela_origem
                    """
                ),
                {
                    "database_origem": database_origem,
                    "tabela_origem": tabela_origem,
                },
            ).mappings().first()

        if not row:
            return datetime(1900, 1, 1), 0

        return row["UltimaAlteracao"], int(row["UltimoNumCad"] or 0)

    def salvar_checkpoint(
        self,
        database_origem: str,
        tabela_origem: str,
        ultima_alteracao: datetime,
        ultimo_numcad: int,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    MERGE [{self.schema}].[MotoristaSyncCheckpoint] AS target
                    USING (
                        SELECT
                            :database_origem AS [DatabaseOrigem],
                            :tabela_origem AS [TabelaOrigem],
                            :ultima_alteracao AS [UltimaAlteracao],
                            :ultimo_numcad AS [UltimoNumCad]
                    ) AS source
                    ON target.[DatabaseOrigem] = source.[DatabaseOrigem]
                    AND target.[TabelaOrigem] = source.[TabelaOrigem]
                    WHEN MATCHED THEN
                        UPDATE SET
                            [UltimaAlteracao] = source.[UltimaAlteracao],
                            [UltimoNumCad] = source.[UltimoNumCad],
                            [AtualizadoEm] = SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN
                        INSERT ([DatabaseOrigem], [TabelaOrigem], [UltimaAlteracao], [UltimoNumCad], [AtualizadoEm])
                        VALUES (source.[DatabaseOrigem], source.[TabelaOrigem], source.[UltimaAlteracao], source.[UltimoNumCad], SYSUTCDATETIME());
                    """
                ),
                {
                    "database_origem": database_origem,
                    "tabela_origem": tabela_origem,
                    "ultima_alteracao": ultima_alteracao,
                    "ultimo_numcad": int(ultimo_numcad),
                },
            )

    def carregar_hashes_por_origem(self, database_origem: str, ids_origem: list[int]) -> dict[int, bytes]:
        ids = sorted({int(v) for v in ids_origem if v is not None})
        if not ids:
            return {}

        params = {"database_origem": database_origem}
        params.update({f"id{i}": valor for i, valor in enumerate(ids)})
        placeholders = ", ".join(f":id{i}" for i in range(len(ids)))

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT [IdDeOrigem], [HashPayload]
                    FROM [{self.schema}].[MotoristaSyncEstado]
                    WHERE [DatabaseOrigem] = :database_origem
                    AND [IdDeOrigem] IN ({placeholders})
                    """
                ),
                params,
            ).mappings().all()

        return {int(row["IdDeOrigem"]): row["HashPayload"] for row in rows}

    def salvar_hashes_por_origem(
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
                    MERGE [{self.schema}].[MotoristaSyncEstado] AS target
                    USING (
                        SELECT
                            :database_origem AS [DatabaseOrigem],
                            :id_de_origem AS [IdDeOrigem],
                            :hash_payload AS [HashPayload]
                    ) AS source
                    ON target.[DatabaseOrigem] = source.[DatabaseOrigem]
                    AND target.[IdDeOrigem] = source.[IdDeOrigem]
                    WHEN MATCHED THEN
                        UPDATE SET
                            [HashPayload] = source.[HashPayload],
                            [AtualizadoEm] = SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN
                        INSERT ([DatabaseOrigem], [IdDeOrigem], [HashPayload], [AtualizadoEm])
                        VALUES (source.[DatabaseOrigem], source.[IdDeOrigem], source.[HashPayload], SYSUTCDATETIME());
                    """
                ),
                [
                    {
                        "database_origem": database_origem,
                        "id_de_origem": int(item["id_de_origem"]),
                        "hash_payload": item["hash_payload"],
                    }
                    for item in hashes
                ],
            )

    def resetar_estado_sync(self, database_origem: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    DELETE FROM [{self.schema}].[MotoristaSyncEstado]
                    WHERE [DatabaseOrigem] = :database_origem
                    """
                ),
                {"database_origem": database_origem},
            )
            conn.execute(
                text(
                    f"""
                    DELETE FROM [{self.schema}].[MotoristaSyncCheckpoint]
                    WHERE [DatabaseOrigem] = :database_origem
                    """
                ),
                {"database_origem": database_origem},
            )

    def inserir_eventos(self, eventos: list[dict[str, Any]]) -> int:
        if not eventos:
            return 0

        colunas = self._carregar_colunas_tabela()
        self._validar_colunas_obrigatorias(colunas)

        mapping_colunas = self._resolver_colunas_para_insert(colunas)
        cols_insert = list(mapping_colunas.keys())

        col_evento_tipo = colunas[_normalize_key("EventoTipo")]
        col_versao_payload = colunas[_normalize_key("VersaoPayload")]
        col_hash_payload = colunas[_normalize_key("HashPayload")]
        col_id_origem = colunas[_normalize_key("IdDeOrigem")]
        col_status = colunas[_normalize_key("Status")]
        col_numemp = colunas.get(_normalize_key("NumEmp"))

        where_keys = [
            f"t.[{col_id_origem}] = :check_id_de_origem",
            f"t.[{col_evento_tipo}] = :check_evento_tipo",
            f"t.[{col_versao_payload}] = :check_versao_payload",
            f"t.[{col_hash_payload}] = :check_hash_payload",
            f"t.[{col_status}] IN ('PENDENTE', 'ERRO')",
        ]
        if col_numemp:
            where_keys.insert(0, f"t.[{col_numemp}] = :check_numemp")

        sql = text(
            f"""
            INSERT INTO [{self.schema}].[{self.table_name}] ({", ".join(f"[{c}]" for c in cols_insert)})
            SELECT {", ".join(f":{p}" for p in mapping_colunas.values())}
            WHERE NOT EXISTS (
                SELECT 1
                FROM [{self.schema}].[{self.table_name}] AS t
                WHERE {' AND '.join(where_keys)}
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
                    if "UX_MotoristaCadastro_Idem" in str(exc):
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
                "Colunas obrigatorias nao encontradas em MotoristaCadastro: "
                + ", ".join(faltantes)
            )

    def _resolver_colunas_para_insert(self, colunas: dict[str, str]) -> dict[str, str]:
        mapping = {
            colunas[_normalize_key("IdDeOrigem")]: "id_de_origem",
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

        for coluna, param in _COLUNAS_ESPELHO_DIRETAS.items():
            key = _normalize_key(coluna)
            if key in colunas:
                mapping[colunas[key]] = param

        for campo, aliases in _CAMPOS_ESPELHO_ALIAS.items():
            for alias in aliases:
                key = _normalize_key(alias)
                if key in colunas:
                    mapping[colunas[key]] = campo
                    break

        return mapping

    @staticmethod
    def _montar_params_evento(mapping_colunas: dict[str, str], evento: dict[str, Any]) -> dict[str, Any]:
        agora = datetime.utcnow()
        base = {
            "id_de_origem": int(evento["id_de_origem"]),
            "operacao": evento["operacao"],
            "evento_tipo": evento.get("evento_tipo") or "MOTORISTA_UPSERT",
            "versao_payload": evento.get("versao_payload") or "v1",
            "hash_payload": evento["hash_payload"],
            "payload_json": evento["payload_json"],
            "status": evento.get("status") or "PENDENTE",
            "tentativas": int(evento.get("tentativas", 0)),
            "origem_tabela": evento.get("origem_tabela") or "R034FUN/R034CPL",
            "criado_em": evento.get("criado_em") or agora,
            "atualizado_em": evento.get("atualizado_em") or agora,
            "origem_sistema": evento.get("origem_sistema") or "Vetorh",
            "usuario_banco": evento.get("usuario_banco"),
            "proxima_tentativa_em": evento.get("proxima_tentativa_em"),
            "ultimo_erro": evento.get("ultimo_erro"),
            "http_status": evento.get("http_status"),
            "resposta_resumo": evento.get("resposta_resumo"),
            "lock_id": evento.get("lock_id"),
            "lock_em": evento.get("lock_em"),
            "processado_em": evento.get("processado_em"),
            "cpf": evento.get("cpf"),
            "matricula": evento.get("matricula"),
            "nome": evento.get("nome"),
            "centro_de_custo": evento.get("centro_de_custo"),
            "tipo_de_colaborador": evento.get("tipo_de_colaborador"),
            "situacao": evento.get("situacao"),
            "nome_do_motorista": evento.get("nome_do_motorista"),
            "numero_do_cpf": evento.get("numero_do_cpf"),
            "data_do_nascimento": evento.get("data_do_nascimento"),
            "sexo": evento.get("sexo"),
            "estado_de_residencia": evento.get("estado_de_residencia"),
            "pais_do_cadastro": evento.get("pais_do_cadastro"),
            "naturalidade": evento.get("naturalidade"),
            "pais": evento.get("pais"),
            "orgao_expedidor_do_rg": evento.get("orgao_expedidor_do_rg"),
            "data_de_emissao_da_cnh": evento.get("data_de_emissao_da_cnh"),
            "data_de_vencimento_da_cnh": evento.get("data_de_vencimento_da_cnh"),
            "numero_do_rg": evento.get("numero_do_rg"),
            "numero_da_cnh": evento.get("numero_da_cnh"),
            "categoria_da_cnh": evento.get("categoria_da_cnh"),
            "numero_do_registro_da_cnh": evento.get("numero_do_registro_da_cnh"),
            "estado_civil": evento.get("estado_civil"),
            "nome_da_mae": evento.get("nome_da_mae"),
            "cidade": evento.get("cidade"),
            "logradouro": evento.get("logradouro"),
            "bairro": evento.get("bairro"),
            "numero_da_residencia": evento.get("numero_da_residencia"),
            "ddd": evento.get("ddd"),
            "numero_de_telefone": evento.get("numero_de_telefone"),
            "numemp": evento.get("numemp"),
        }

        params = {
            param_name: base.get(param_name)
            for param_name in set(mapping_colunas.values())
        }
        params["check_numemp"] = base.get("numemp")
        params["check_id_de_origem"] = base["id_de_origem"]
        params["check_evento_tipo"] = base["evento_tipo"]
        params["check_versao_payload"] = base["versao_payload"]
        params["check_hash_payload"] = base["hash_payload"]
        return params
