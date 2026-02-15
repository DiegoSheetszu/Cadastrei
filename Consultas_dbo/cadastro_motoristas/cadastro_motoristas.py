import re
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.engine import Engine

from Consultas_dbo.query import (
    montar_query_cadastro_motoristas,
    montar_query_cadastro_motoristas_por_numcads,
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_CANDIDATOS_DATA_HORA = (
    ("DatAlt", "HorAlt"),
    ("DatAtu", "HorAtu"),
    ("DatInc", "HorInc"),
    ("DatCad", "HorCad"),
    ("DatAdm", None),
)


def _safe_identifier(value: str, label: str) -> str:
    normalized = (value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"{label} invalido: {value!r}")
    return normalized


class RepositorioCadastroMotoristas:
    def __init__(self, engine: Engine, schema_origem: str = "dbo"):
        self.engine = engine
        self.schema_origem = _safe_identifier(schema_origem, "Schema de origem")
        self._cache_colunas_data_hora: dict[str, tuple[str, str | None]] = {}

    def buscar_dados_cadastro_motoristas(self, limit: int = 1) -> List[Dict[str, Any]]:
        query = montar_query_cadastro_motoristas(self.schema_origem)
        with self.engine.connect() as conn:
            rows = conn.execute(
                query,
                {"limit": limit},
            ).mappings().all()
            return [dict(r) for r in rows]

    def buscar_dados_cadastro_motoristas_por_numcads(self, numcads: list[int]) -> List[Dict[str, Any]]:
        ids = sorted({int(n) for n in numcads if n is not None})
        if not ids:
            return []

        params = {f"id{i}": numcad for i, numcad in enumerate(ids)}
        placeholders = ", ".join(f":id{i}" for i in range(len(ids)))
        query = montar_query_cadastro_motoristas_por_numcads(self.schema_origem, placeholders)

        with self.engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()
            return [dict(r) for r in rows]

    def buscar_numcads_alterados(
        self,
        tabela_origem: str,
        limite: int,
        ultima_alteracao: datetime,
        ultimo_numcad: int,
    ) -> list[dict[str, Any]]:
        tabela = _safe_identifier(tabela_origem, "Tabela de origem").upper()
        if tabela not in {"R034FUN", "R034CPL"}:
            raise ValueError(f"Tabela de origem nao suportada: {tabela_origem!r}")

        try:
            coluna_data, coluna_hora = self._resolver_colunas_data_hora(tabela)
        except ValueError:
            if tabela == "R034CPL":
                return self._varrer_numcads_ativos_por_cursor(
                    limite=max(1, int(limite)),
                    ultimo_numcad=int(ultimo_numcad),
                )
            raise

        expr = self._expressao_alteracao(alias="t", coluna_data=coluna_data, coluna_hora=coluna_hora)
        tabela_qualificada = f"[{self.schema_origem}].[{tabela}]"
        r034fun = f"[{self.schema_origem}].[R034FUN]"

        if tabela == "R034FUN":
            sql = text(
                f"""
                WITH BASE AS (
                    SELECT
                        t.[NumCad] AS numcad,
                        {expr} AS change_dt
                    FROM {tabela_qualificada} AS t
                    WHERE t.[SitAfa] NOT IN (7)
                    AND t.[TipCol] = 1
                    AND t.[CodCar] = 152292
                )
                SELECT TOP (:limite)
                    b.numcad,
                    b.change_dt
                FROM BASE AS b
                WHERE b.change_dt IS NOT NULL
                AND (
                    b.change_dt > :ultima_alteracao
                    OR (b.change_dt = :ultima_alteracao AND b.numcad > :ultimo_numcad)
                )
                ORDER BY b.change_dt ASC, b.numcad ASC
                """
            )
        else:
            sql = text(
                f"""
                WITH BASE AS (
                    SELECT
                        t.[NumCad] AS numcad,
                        MAX({expr}) AS change_dt
                    FROM {tabela_qualificada} AS t
                    INNER JOIN {r034fun} AS f
                        ON f.[NumCad] = t.[NumCad]
                    WHERE f.[SitAfa] NOT IN (7)
                    AND f.[TipCol] = 1
                    AND f.[CodCar] = 152292
                    GROUP BY t.[NumCad]
                )
                SELECT TOP (:limite)
                    b.numcad,
                    b.change_dt
                FROM BASE AS b
                WHERE b.change_dt IS NOT NULL
                AND (
                    b.change_dt > :ultima_alteracao
                    OR (b.change_dt = :ultima_alteracao AND b.numcad > :ultimo_numcad)
                )
                ORDER BY b.change_dt ASC, b.numcad ASC
                """
            )

        with self.engine.connect() as conn:
            rows = conn.execute(
                sql,
                {
                    "limite": max(1, int(limite)),
                    "ultima_alteracao": ultima_alteracao,
                    "ultimo_numcad": int(ultimo_numcad),
                },
            ).mappings().all()
            return [dict(r) for r in rows]

    def _varrer_numcads_ativos_por_cursor(self, limite: int, ultimo_numcad: int) -> list[dict[str, Any]]:
        r034fun = f"[{self.schema_origem}].[R034FUN]"
        sql = text(
            f"""
            SELECT TOP (:limite)
                f.[NumCad] AS numcad,
                CAST('1900-01-01' AS DATETIME2(0)) AS change_dt
            FROM {r034fun} AS f
            WHERE f.[SitAfa] NOT IN (7)
            AND f.[TipCol] = 1
            AND f.[CodCar] = 152292
            AND f.[NumCad] > :ultimo_numcad
            ORDER BY f.[NumCad] ASC
            """
        )

        params = {
            "limite": max(1, int(limite)),
            "ultimo_numcad": max(0, int(ultimo_numcad)),
        }

        with self.engine.connect() as conn:
            rows = conn.execute(sql, params).mappings().all()
            if rows:
                return [dict(r) for r in rows]

            if params["ultimo_numcad"] <= 0:
                return []

            rows_reinicio = conn.execute(
                sql,
                {
                    "limite": params["limite"],
                    "ultimo_numcad": 0,
                },
            ).mappings().all()
            return [dict(r) for r in rows_reinicio]

    def _resolver_colunas_data_hora(self, tabela_origem: str) -> tuple[str, str | None]:
        tabela = tabela_origem.upper()
        if tabela in self._cache_colunas_data_hora:
            return self._cache_colunas_data_hora[tabela]

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
                {"schema": self.schema_origem, "tabela": tabela},
            ).scalars().all()

        lookup = {str(nome).lower(): str(nome) for nome in colunas}

        for candidato_data, candidato_hora in _CANDIDATOS_DATA_HORA:
            coluna_data = lookup.get(candidato_data.lower())
            if not coluna_data:
                continue
            coluna_hora = lookup.get(candidato_hora.lower()) if candidato_hora else None
            self._cache_colunas_data_hora[tabela] = (coluna_data, coluna_hora)
            return coluna_data, coluna_hora

        raise ValueError(
            f"Nao foi encontrada coluna de auditoria em [{self.schema_origem}].[{tabela}]"
        )

    @staticmethod
    def _expressao_alteracao(alias: str, coluna_data: str, coluna_hora: str | None) -> str:
        if not coluna_hora:
            return f"CAST({alias}.[{coluna_data}] AS DATETIME2(0))"

        return (
            f"CASE "
            f"WHEN {alias}.[{coluna_hora}] IS NULL THEN CAST({alias}.[{coluna_data}] AS DATETIME2(0)) "
            f"ELSE DATEADD(MINUTE, "
            f"((TRY_CONVERT(INT, {alias}.[{coluna_hora}]) / 100) * 60) + (TRY_CONVERT(INT, {alias}.[{coluna_hora}]) % 100), "
            f"CAST({alias}.[{coluna_data}] AS DATETIME2(0))) "
            f"END"
        )
