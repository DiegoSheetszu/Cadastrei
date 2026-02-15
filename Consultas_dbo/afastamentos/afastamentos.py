from typing import Any, Dict, List
from datetime import date, datetime

from sqlalchemy.engine import Engine
from Consultas_dbo.query import (
    montar_query_afastamentos,
    montar_query_afastamentos_por_cursor,
)


class RepositorioAfastamentos:
    def __init__(self, engine: Engine, schema_origem: str = "dbo"):
        self.engine = engine
        self.schema_origem = schema_origem

    def buscar_dados_afastamentos(
        self,
        *,
        data_inicio: date,
        since: str | None = None,
        limit: int = 1,
    ) -> List[Dict[str, Any]]:
        query = montar_query_afastamentos(self.schema_origem)
        with self.engine.connect() as conn:
            rows = conn.execute(
                query,
                {
                    "limit": limit,
                    "data_inicio": data_inicio,
                },
            ).mappings().all()
            return [dict(r) for r in rows]

    def buscar_dados_afastamentos_por_cursor(
        self,
        *,
        limit: int,
        c_numemp: int,
        c_tipcol: int,
        c_numcad: int,
        c_datafa: datetime,
        c_horafa: int,
        c_seqreg: int,
        data_inicio: date,
    ) -> List[Dict[str, Any]]:
        query = montar_query_afastamentos_por_cursor(self.schema_origem)
        with self.engine.connect() as conn:
            rows = conn.execute(
                query,
                {
                    "limit": max(1, int(limit)),
                    "c_numemp": int(c_numemp),
                    "c_tipcol": int(c_tipcol),
                    "c_numcad": int(c_numcad),
                    "c_datafa": c_datafa,
                    "c_horafa": int(c_horafa),
                    "c_seqreg": int(c_seqreg),
                    "data_inicio": data_inicio,
                },
            ).mappings().all()
            return [dict(r) for r in rows]
