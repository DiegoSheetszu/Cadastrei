from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CHECK_LITERAL_RE = re.compile(r"N?'([^']+)'", re.IGNORECASE)


def _safe_identifier(value: str, label: str) -> str:
    normalized = (value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"{label} invalido: {value!r}")
    return normalized


def _normalize_key(value: str) -> str:
    return "".join(ch for ch in str(value).lower() if ch.isalnum())


class RepositorioFilaIntegracaoApi:
    def __init__(
        self,
        engine: Engine,
        *,
        schema: str = "dbo",
        tabela_motorista: str = "MotoristaCadastro",
        tabela_afastamento: str = "Afastamento",
    ) -> None:
        self.engine = engine
        self.schema = _safe_identifier(schema, "Schema")
        self.tabela_motorista = _safe_identifier(tabela_motorista, "Tabela de motoristas")
        self.tabela_afastamento = _safe_identifier(tabela_afastamento, "Tabela de afastamentos")
        self._cache_colunas: dict[str, dict[str, str]] = {}
        self._cache_status_sucesso: dict[str, list[str]] = {}
        self._cache_tabela_empresa_dict: dict[str, str] | None = None
        self._cache_tabela_sindicato_dict: dict[str, str] | None = None

    def liberar_locks_expirados(self, lock_timeout_minutes: int = 15) -> dict[str, int]:
        return {
            "motoristas": self._liberar_locks_expirados_tabela(
                self.tabela_motorista,
                lock_timeout_minutes=lock_timeout_minutes,
            ),
            "afastamentos": self._liberar_locks_expirados_tabela(
                self.tabela_afastamento,
                lock_timeout_minutes=lock_timeout_minutes,
            ),
        }

    def liberar_locks_expirados_motoristas(self, lock_timeout_minutes: int = 15) -> int:
        return self._liberar_locks_expirados_tabela(
            self.tabela_motorista,
            lock_timeout_minutes=lock_timeout_minutes,
        )

    def liberar_locks_expirados_afastamentos(self, lock_timeout_minutes: int = 15) -> int:
        return self._liberar_locks_expirados_tabela(
            self.tabela_afastamento,
            lock_timeout_minutes=lock_timeout_minutes,
        )

    def capturar_motoristas_pendentes(
        self,
        *,
        lock_id: str,
        batch_size: int,
        max_tentativas: int,
        lock_timeout_minutes: int,
    ) -> list[dict[str, Any]]:
        return self._capturar_lote(
            table_name=self.tabela_motorista,
            lock_id=lock_id,
            batch_size=batch_size,
            max_tentativas=max_tentativas,
            lock_timeout_minutes=lock_timeout_minutes,
            key_columns={
                "id_de_origem": "IdDeOrigem",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
            },
            optional_key_columns={
                "numemp": "NumEmp",
                "numero_sindicato": "NumeroSindicato",
            },
        )

    def capturar_afastamentos_pendentes(
        self,
        *,
        lock_id: str,
        batch_size: int,
        max_tentativas: int,
        lock_timeout_minutes: int,
    ) -> list[dict[str, Any]]:
        return self._capturar_lote(
            table_name=self.tabela_afastamento,
            lock_id=lock_id,
            batch_size=batch_size,
            max_tentativas=max_tentativas,
            lock_timeout_minutes=lock_timeout_minutes,
            key_columns={
                "numempresa": "NumeroDaEmpresa",
                "tipocolaborador": "TipoDeColaborador",
                "numorigem": "NumeroDeOrigemDoColaborador",
                "dataafastamento": "DataDoAfastamento",
                "situacao": "Situacao",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
            },
            optional_key_columns={"numero_sindicato": "NumeroSindicato"},
        )

    def buscar_colunas_motorista_por_evento(
        self,
        *,
        evento: dict[str, Any],
        colunas: list[str],
    ) -> dict[str, Any]:
        return self._buscar_colunas_evento(
            table_name=self.tabela_motorista,
            evento=evento,
            requested_columns=colunas,
            key_columns={
                "id_de_origem": "IdDeOrigem",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
            },
            optional_key_columns={
                "numemp": "NumEmp",
                "numero_sindicato": "NumeroSindicato",
            },
        )

    def buscar_colunas_afastamento_por_evento(
        self,
        *,
        evento: dict[str, Any],
        colunas: list[str],
    ) -> dict[str, Any]:
        return self._buscar_colunas_evento(
            table_name=self.tabela_afastamento,
            evento=evento,
            requested_columns=colunas,
            key_columns={
                "numempresa": "NumeroDaEmpresa",
                "tipocolaborador": "TipoDeColaborador",
                "numorigem": "NumeroDeOrigemDoColaborador",
                "dataafastamento": "DataDoAfastamento",
                "situacao": "Situacao",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
            },
            optional_key_columns={"numero_sindicato": "NumeroSindicato"},
        )

    def buscar_pessoa_juridica_por_codigo(
        self,
        *,
        codigo_empresa: int | None,
        tipo_pessoa: str,
        cliente_api_id: str | None = None,  # compatibilidade (nao utilizado no Dict simples)
        ambiente: str | None = None,        # compatibilidade (nao utilizado no Dict simples)
    ) -> dict[str, Any] | None:
        if codigo_empresa is None:
            return None

        tipo = str(tipo_pessoa or "").strip().upper()
        if tipo == "EMPREGADOR":
            resolved = self._resolver_colunas_empresa_dict()
            table_name = "EmpresaDict"
        elif tipo == "SINDICATO":
            resolved = self._resolver_colunas_sindicato_dict()
            table_name = "SindicatoDict"
        else:
            return None
        if not resolved:
            return None

        where_parts = [
            f"t.[{resolved['codigo']}] = :codigo",
        ]
        if "ativo" in resolved:
            where_parts.append(f"ISNULL(t.[{resolved['ativo']}], 1) = 1")

        select_parts = [
            f"t.[{resolved['nome']}] AS [nome]",
            f"t.[{resolved['cnpj']}] AS [cnpj]",
            f"t.[{resolved['cidade']}] AS [cidade]",
            f"t.[{resolved['uf']}] AS [uf]",
        ]
        select_parts.append(f"t.[{resolved['codigo']}] AS [codigo_pessoa]")
        for opt_alias in ("rua", "numero", "complemento", "bairro", "cep", "latitude", "longitude"):
            if opt_alias in resolved:
                select_parts.append(f"t.[{resolved[opt_alias]}] AS [{opt_alias}]")

        order_parts: list[str] = []
        if "atualizado_em" in resolved:
            order_parts.append(f"t.[{resolved['atualizado_em']}] DESC")
        order_parts.append(f"t.[{resolved['codigo']}] ASC")

        params: dict[str, Any] = {
            "codigo": int(codigo_empresa),
        }

        sql = text(
            f"""
            SELECT TOP 1
                {', '.join(select_parts)}
            FROM [{self.schema}].[{table_name}] AS t
            WHERE {' AND '.join(where_parts)}
            ORDER BY {', '.join(order_parts)}
            """
        )

        with self.engine.connect() as conn:
            row = conn.execute(sql, params).mappings().first()
        if not row:
            return None
        return dict(row)

    def marcar_motorista_sucesso(
        self,
        *,
        evento: dict[str, Any],
        lock_id: str,
        http_status: int | None,
        resposta_resumo: str | None,
    ) -> bool:
        return self._marcar_resultado(
            table_name=self.tabela_motorista,
            lock_id=lock_id,
            evento=evento,
            sucesso=True,
            http_status=http_status,
            resposta_resumo=resposta_resumo,
            ultimo_erro=None,
            proxima_tentativa=None,
            key_columns={
                "id_de_origem": "IdDeOrigem",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
            },
            optional_key_columns={"numemp": "NumEmp"},
        )

    def marcar_motorista_erro(
        self,
        *,
        evento: dict[str, Any],
        lock_id: str,
        http_status: int | None,
        resposta_resumo: str | None,
        ultimo_erro: str,
        proxima_tentativa: datetime | None,
    ) -> bool:
        return self._marcar_resultado(
            table_name=self.tabela_motorista,
            lock_id=lock_id,
            evento=evento,
            sucesso=False,
            http_status=http_status,
            resposta_resumo=resposta_resumo,
            ultimo_erro=ultimo_erro,
            proxima_tentativa=proxima_tentativa,
            key_columns={
                "id_de_origem": "IdDeOrigem",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
            },
            optional_key_columns={"numemp": "NumEmp"},
        )

    def marcar_afastamento_sucesso(
        self,
        *,
        evento: dict[str, Any],
        lock_id: str,
        http_status: int | None,
        resposta_resumo: str | None,
    ) -> bool:
        return self._marcar_resultado(
            table_name=self.tabela_afastamento,
            lock_id=lock_id,
            evento=evento,
            sucesso=True,
            http_status=http_status,
            resposta_resumo=resposta_resumo,
            ultimo_erro=None,
            proxima_tentativa=None,
            key_columns={
                "numempresa": "NumeroDaEmpresa",
                "tipocolaborador": "TipoDeColaborador",
                "numorigem": "NumeroDeOrigemDoColaborador",
                "dataafastamento": "DataDoAfastamento",
                "situacao": "Situacao",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
            },
        )

    def marcar_afastamento_erro(
        self,
        *,
        evento: dict[str, Any],
        lock_id: str,
        http_status: int | None,
        resposta_resumo: str | None,
        ultimo_erro: str,
        proxima_tentativa: datetime | None,
    ) -> bool:
        return self._marcar_resultado(
            table_name=self.tabela_afastamento,
            lock_id=lock_id,
            evento=evento,
            sucesso=False,
            http_status=http_status,
            resposta_resumo=resposta_resumo,
            ultimo_erro=ultimo_erro,
            proxima_tentativa=proxima_tentativa,
            key_columns={
                "numempresa": "NumeroDaEmpresa",
                "tipocolaborador": "TipoDeColaborador",
                "numorigem": "NumeroDeOrigemDoColaborador",
                "dataafastamento": "DataDoAfastamento",
                "situacao": "Situacao",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
            },
        )

    def _buscar_colunas_evento(
        self,
        *,
        table_name: str,
        evento: dict[str, Any],
        requested_columns: list[str],
        key_columns: dict[str, str],
        optional_key_columns: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        cleaned_columns: list[str] = []
        seen_columns: set[str] = set()
        for column_name in requested_columns:
            logical = str(column_name or "").strip()
            if not logical:
                continue
            norm = _normalize_key(logical)
            if not norm or norm in seen_columns:
                continue
            seen_columns.add(norm)
            cleaned_columns.append(logical)

        if not cleaned_columns:
            return {}

        requested_aliases = {f"sel_{idx}": col for idx, col in enumerate(cleaned_columns)}
        resolved = self._resolver_colunas(
            table_name,
            required_columns={**key_columns},
            optional_columns={**(optional_key_columns or {}), **requested_aliases},
        )

        select_parts: list[str] = []
        for alias in requested_aliases.keys():
            if alias in resolved:
                select_parts.append(f"t.[{resolved[alias]}] AS [{alias}]")
            else:
                select_parts.append(f"NULL AS [{alias}]")
        where_parts: list[str] = []
        params: dict[str, Any] = {}

        for alias in key_columns.keys():
            value = evento.get(alias)
            if value is None:
                where_parts.append(f"t.[{resolved[alias]}] IS NULL")
            else:
                where_parts.append(f"t.[{resolved[alias]}] = :{alias}")
                params[alias] = value

        for alias in (optional_key_columns or {}).keys():
            if alias not in resolved:
                continue
            value = evento.get(alias)
            if value is None:
                continue
            where_parts.append(f"t.[{resolved[alias]}] = :{alias}")
            params[alias] = value

        if not where_parts:
            return {}

        sql = text(
            f"""
            SELECT TOP 1
                {', '.join(select_parts)}
            FROM [{self.schema}].[{table_name}] AS t
            WHERE {' AND '.join(where_parts)}
            """
        )

        with self.engine.connect() as conn:
            row = conn.execute(sql, params).mappings().first()

        if not row:
            return {}

        return {
            logical_name: row.get(alias_name)
            for alias_name, logical_name in requested_aliases.items()
        }

    def _capturar_lote(
        self,
        *,
        table_name: str,
        lock_id: str,
        batch_size: int,
        max_tentativas: int,
        lock_timeout_minutes: int,
        key_columns: dict[str, str],
        optional_key_columns: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        resolved = self._resolver_colunas(
            table_name,
            required_columns={
                **key_columns,
                "payload_json": "PayloadJson",
                "tentativas": "Tentativas",
                "status": "Status",
                "lock_id": "LockId",
                "lock_em": "LockEm",
                "criado_em": "CriadoEm",
            },
            optional_columns={
                **(optional_key_columns or {}),
                "proxima_tentativa_em": "ProximaTentativaEm",
                "atualizado_em": "AtualizadoEm",
            },
        )

        key_aliases = list(key_columns.keys()) + [
            alias
            for alias in (optional_key_columns or {}).keys()
            if alias in resolved
        ]
        output_aliases = key_aliases + ["payload_json", "tentativas"]

        select_parts: list[str] = []
        selected_aliases: set[str] = set()
        for alias in output_aliases:
            select_parts.append(f"t.[{resolved[alias]}] AS [{alias}]")
            selected_aliases.add(alias)

        # Colunas que serao alteradas no UPDATE precisam fazer parte do CTE.
        for alias in ("status", "lock_id", "lock_em", "atualizado_em"):
            if alias not in resolved:
                continue
            actual_name = resolved[alias]
            if actual_name in selected_aliases:
                continue
            select_parts.append(f"t.[{actual_name}] AS [{actual_name}]")
            selected_aliases.add(actual_name)

        select_cols = ",\n                        ".join(select_parts)
        # Em UPDATE sobre CTE, a pseudo-tabela INSERTED expõe os nomes do CTE
        # (aliases selecionados), não necessariamente os nomes físicos da tabela.
        output_cols = ",\n                    ".join(
            f"INSERTED.[{alias}] AS [{alias}]"
            for alias in output_aliases
        )

        where_parts = [
            f"t.[{resolved['status']}] IN ('PENDENTE', 'ERRO')",
            f"ISNULL(t.[{resolved['tentativas']}], 0) < :max_tentativas",
            (
                f"(t.[{resolved['lock_id']}] IS NULL "
                f"OR t.[{resolved['lock_em']}] < DATEADD(MINUTE, -:lock_timeout_minutes, SYSUTCDATETIME()))"
            ),
        ]
        if "proxima_tentativa_em" in resolved:
            where_parts.append(
                f"(t.[{resolved['proxima_tentativa_em']}] IS NULL "
                f"OR t.[{resolved['proxima_tentativa_em']}] <= SYSUTCDATETIME())"
            )

        order_parts = []
        if "proxima_tentativa_em" in resolved:
            order_parts.append(
                f"ISNULL(t.[{resolved['proxima_tentativa_em']}], t.[{resolved['criado_em']}]) ASC"
            )
        order_parts.append(f"t.[{resolved['criado_em']}] ASC")
        for alias in key_aliases:
            order_parts.append(f"t.[{resolved[alias]}] ASC")

        set_parts = [
            f"[{resolved['status']}] = 'PROCESSANDO'",
            f"[{resolved['lock_id']}] = :lock_id",
            f"[{resolved['lock_em']}] = SYSUTCDATETIME()",
        ]
        if "atualizado_em" in resolved:
            set_parts.append(f"[{resolved['atualizado_em']}] = SYSUTCDATETIME()")

        sql = text(
            f"""
            WITH lote AS (
                SELECT TOP (:batch_size)
                    {select_cols}
                FROM [{self.schema}].[{table_name}] AS t WITH (ROWLOCK, UPDLOCK, READPAST)
                WHERE {' AND '.join(where_parts)}
                ORDER BY {', '.join(order_parts)}
            )
            UPDATE lote
            SET {', '.join(set_parts)}
            OUTPUT
                {output_cols}
            ;
            """
        )

        with self.engine.begin() as conn:
            rows = conn.execute(
                sql,
                {
                    "batch_size": max(1, int(batch_size)),
                    "max_tentativas": max(1, int(max_tentativas)),
                    "lock_timeout_minutes": max(1, int(lock_timeout_minutes)),
                    "lock_id": lock_id,
                },
            ).mappings().all()
        return [dict(row) for row in rows]

    def _marcar_resultado(
        self,
        *,
        table_name: str,
        lock_id: str,
        evento: dict[str, Any],
        sucesso: bool,
        http_status: int | None,
        resposta_resumo: str | None,
        ultimo_erro: str | None,
        proxima_tentativa: datetime | None,
        key_columns: dict[str, str],
        optional_key_columns: dict[str, str] | None = None,
    ) -> bool:
        resolved = self._resolver_colunas(
            table_name,
            required_columns={
                **key_columns,
                "status": "Status",
                "tentativas": "Tentativas",
                "lock_id": "LockId",
                "lock_em": "LockEm",
            },
            optional_columns={
                **(optional_key_columns or {}),
                "http_status": "HttpStatus",
                "resposta_resumo": "RespostaResumo",
                "ultimo_erro": "UltimoErro",
                "proxima_tentativa_em": "ProximaTentativaEm",
                "processado_em": "ProcessadoEm",
                "atualizado_em": "AtualizadoEm",
            },
        )

        set_parts = [
            f"t.[{resolved['status']}] = :status",
            f"t.[{resolved['tentativas']}] = ISNULL(t.[{resolved['tentativas']}], 0) + 1",
            f"t.[{resolved['lock_id']}] = NULL",
            f"t.[{resolved['lock_em']}] = NULL",
        ]
        params: dict[str, Any] = {
            "lock_id": lock_id,
        }

        if "http_status" in resolved:
            set_parts.append(f"t.[{resolved['http_status']}] = :http_status")
            params["http_status"] = http_status

        if "resposta_resumo" in resolved:
            set_parts.append(f"t.[{resolved['resposta_resumo']}] = :resposta_resumo")
            params["resposta_resumo"] = resposta_resumo

        if "ultimo_erro" in resolved:
            set_parts.append(f"t.[{resolved['ultimo_erro']}] = :ultimo_erro")
            params["ultimo_erro"] = None if sucesso else ultimo_erro

        if "proxima_tentativa_em" in resolved:
            set_parts.append(f"t.[{resolved['proxima_tentativa_em']}] = :proxima_tentativa")
            params["proxima_tentativa"] = None if sucesso else proxima_tentativa

        if "processado_em" in resolved:
            set_parts.append(
                f"t.[{resolved['processado_em']}] = "
                f"{'SYSUTCDATETIME()' if sucesso else 'NULL'}"
            )

        if "atualizado_em" in resolved:
            set_parts.append(f"t.[{resolved['atualizado_em']}] = SYSUTCDATETIME()")

        where_parts = [f"t.[{resolved['lock_id']}] = :lock_id"]
        for alias in list(key_columns.keys()) + list((optional_key_columns or {}).keys()):
            if alias not in resolved:
                continue
            where_parts.append(f"t.[{resolved[alias]}] = :{alias}")
            params[alias] = evento.get(alias)

        sql = text(
            f"""
            UPDATE t
            SET {', '.join(set_parts)}
            FROM [{self.schema}].[{table_name}] AS t
            WHERE {' AND '.join(where_parts)}
            """
        )

        status_candidates = (
            self._status_sucesso_candidates(table_name)
            if sucesso
            else ["ERRO"]
        )

        last_exc: Exception | None = None
        for status_value in status_candidates:
            params["status"] = status_value
            try:
                with self.engine.begin() as conn:
                    result = conn.execute(sql, params)
                    return int(result.rowcount or 0) > 0
            except IntegrityError as exc:
                # Alguns ambientes usam CHECK de status diferente
                # (ex.: ENVIADO no lugar de PROCESSADO).
                if not sucesso or not self._is_status_constraint_error(exc):
                    raise
                last_exc = exc
                continue

        if last_exc is not None:
            raise last_exc
        return False

    def _status_sucesso_candidates(self, table_name: str) -> list[str]:
        if table_name in self._cache_status_sucesso:
            return self._cache_status_sucesso[table_name]

        allowed = self._status_values_from_constraints(table_name)
        preferred = ["PROCESSADO", "ENVIADO", "INTEGRADO", "CONCLUIDO", "SUCESSO", "OK"]
        blocked = {"PENDENTE", "PROCESSANDO", "ERRO"}

        candidates: list[str] = []
        for value in preferred:
            if allowed and value not in allowed:
                continue
            if value not in candidates:
                candidates.append(value)

        if allowed:
            for value in allowed:
                if value in blocked:
                    continue
                if value not in candidates:
                    candidates.append(value)

        if not candidates:
            candidates = ["PROCESSADO"]

        self._cache_status_sucesso[table_name] = candidates
        return candidates

    def _status_values_from_constraints(self, table_name: str) -> list[str]:
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT cc.[definition]
                        FROM sys.check_constraints AS cc
                        INNER JOIN sys.tables AS t
                            ON t.[object_id] = cc.[parent_object_id]
                        INNER JOIN sys.schemas AS s
                            ON s.[schema_id] = t.[schema_id]
                        WHERE s.[name] = :schema
                        AND t.[name] = :table_name
                        """
                    ),
                    {"schema": self.schema, "table_name": table_name},
                ).scalars().all()
        except Exception:
            return []

        values: list[str] = []
        seen: set[str] = set()
        for raw_def in rows:
            definition = str(raw_def or "")
            if "status" not in definition.lower():
                continue
            for match in _CHECK_LITERAL_RE.findall(definition):
                token = str(match or "").strip().upper()
                if not token or token in seen:
                    continue
                seen.add(token)
                values.append(token)
        return values

    @staticmethod
    def _is_status_constraint_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return ("constraint" in message and "status" in message) or "ck_" in message

    def _liberar_locks_expirados_tabela(
        self,
        table_name: str,
        *,
        lock_timeout_minutes: int,
    ) -> int:
        resolved = self._resolver_colunas(
            table_name,
            required_columns={
                "status": "Status",
                "lock_id": "LockId",
                "lock_em": "LockEm",
            },
            optional_columns={
                "ultimo_erro": "UltimoErro",
                "atualizado_em": "AtualizadoEm",
            },
        )

        set_parts = [
            f"[{resolved['status']}] = 'ERRO'",
            f"[{resolved['lock_id']}] = NULL",
            f"[{resolved['lock_em']}] = NULL",
        ]
        params = {"lock_timeout_minutes": max(1, int(lock_timeout_minutes))}

        if "ultimo_erro" in resolved:
            set_parts.append(
                f"[{resolved['ultimo_erro']}] = "
                "'Lock expirado durante processamento. Evento reenfileirado automaticamente.'"
            )
        if "atualizado_em" in resolved:
            set_parts.append(f"[{resolved['atualizado_em']}] = SYSUTCDATETIME()")

        sql = text(
            f"""
            UPDATE [{self.schema}].[{table_name}]
            SET {', '.join(set_parts)}
            WHERE [{resolved['status']}] = 'PROCESSANDO'
            AND [{resolved['lock_id']}] IS NOT NULL
            AND [{resolved['lock_em']}] < DATEADD(MINUTE, -:lock_timeout_minutes, SYSUTCDATETIME())
            """
        )

        with self.engine.begin() as conn:
            result = conn.execute(sql, params)
            return int(result.rowcount or 0)

    def _resolver_colunas(
        self,
        table_name: str,
        *,
        required_columns: dict[str, str],
        optional_columns: dict[str, str] | None = None,
    ) -> dict[str, str]:
        lookup = self._carregar_colunas_tabela(table_name)
        resolved: dict[str, str] = {}

        for alias, logical_name in required_columns.items():
            key = _normalize_key(logical_name)
            if key not in lookup:
                raise ValueError(
                    f"Coluna obrigatoria nao encontrada em [{self.schema}].[{table_name}]: {logical_name}"
                )
            resolved[alias] = lookup[key]

        for alias, logical_name in (optional_columns or {}).items():
            key = _normalize_key(logical_name)
            if key in lookup:
                resolved[alias] = lookup[key]

        return resolved

    def _resolver_colunas_empresa_dict(self) -> dict[str, str] | None:
        if self._cache_tabela_empresa_dict is not None:
            return self._cache_tabela_empresa_dict

        try:
            resolved = self._resolver_colunas(
                "EmpresaDict",
                required_columns={
                    "codigo": "NumEmp",
                    "nome": "Nome",
                    "cnpj": "Cnpj",
                    "cidade": "Cidade",
                    "uf": "Uf",
                },
                optional_columns={
                    "ativo": "Ativo",
                    "atualizado_em": "AtualizadoEm",
                    "rua": "Rua",
                    "numero": "Numero",
                    "complemento": "Complemento",
                    "bairro": "Bairro",
                    "cep": "Cep",
                    "latitude": "Latitude",
                    "longitude": "Longitude",
                },
            )
        except Exception:
            self._cache_tabela_empresa_dict = {}
            return None

        self._cache_tabela_empresa_dict = resolved
        return resolved

    def _resolver_colunas_sindicato_dict(self) -> dict[str, str] | None:
        if self._cache_tabela_sindicato_dict is not None:
            return self._cache_tabela_sindicato_dict

        try:
            resolved = self._resolver_colunas(
                "SindicatoDict",
                required_columns={
                    "codigo": "NumeroSindicato",
                    "nome": "Nome",
                    "cnpj": "Cnpj",
                    "cidade": "Cidade",
                    "uf": "Uf",
                },
                optional_columns={
                    "ativo": "Ativo",
                    "atualizado_em": "AtualizadoEm",
                    "rua": "Rua",
                    "numero": "Numero",
                    "complemento": "Complemento",
                    "bairro": "Bairro",
                    "cep": "Cep",
                    "latitude": "Latitude",
                    "longitude": "Longitude",
                },
            )
        except Exception:
            self._cache_tabela_sindicato_dict = {}
            return None

        self._cache_tabela_sindicato_dict = resolved
        return resolved

    def _carregar_colunas_tabela(self, table_name: str) -> dict[str, str]:
        if table_name in self._cache_colunas:
            return self._cache_colunas[table_name]

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT c.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS AS c
                    WHERE c.TABLE_SCHEMA = :schema
                    AND c.TABLE_NAME = :table_name
                    """
                ),
                {"schema": self.schema, "table_name": table_name},
            ).scalars().all()

        if not rows:
            raise ValueError(f"Tabela nao encontrada: [{self.schema}].[{table_name}]")

        mapped = {_normalize_key(col): col for col in rows}
        self._cache_colunas[table_name] = mapped
        return mapped
