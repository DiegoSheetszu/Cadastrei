from typing import Any, Dict, List
from sqlalchemy.engine import Engine
from Consultas_dbo.query import montar_query_afastamentos


class RepositorioAfastamentos:
    def __init__(self, engine: Engine, schema_origem: str = "dbo"):
        self.engine = engine
        self.schema_origem = schema_origem

    def buscar_dados_afastamentos(self, since: str | None = None, limit: int = 1) -> List[Dict[str, Any]]:
        query = montar_query_afastamentos(self.schema_origem)
        with self.engine.connect() as conn:
            rows = conn.execute(
                query,
                {"limit": limit}
            ).mappings().all()
            return [dict(r) for r in rows]
