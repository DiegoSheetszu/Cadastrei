from typing import Any, Dict, List

from sqlalchemy.engine import Engine

from Consultas_dbo.query import cadastro_motoristas


class RepositorioCadastroMotoristas:
    def __init__(self, engine: Engine):
        self.engine = engine

    def buscar_dados_cadastro_motoristas(self, limit: int = 1) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                cadastro_motoristas,
                {"limit": limit},
            ).mappings().all()
            return [dict(r) for r in rows]
