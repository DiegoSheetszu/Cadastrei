from typing import Any, Dict, List
from sqlalchemy.engine import Engine
from Consultas_dbo.query import afastamentos


class RepositorioAfastamentos:
    def __init__(self, engine: Engine):
        self.engine = engine

    def buscar_dados_afastamentos(self, since: str | None = None, limit: int = 1) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                afastamentos,
                {"limit": limit}
            ).mappings().all()
            return [dict(r) for r in rows]
