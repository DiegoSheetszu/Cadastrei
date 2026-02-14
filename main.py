import json
from datetime import date, datetime
from decimal import Decimal

from config.engine import ativar_engine
from Consultas_dbo.afastamentos.afastamentos import RepositorioAfastamentos


def _to_yyyy_mm_dd(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    return text[:10]


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float, Decimal)):
        return value != 0

    text = str(value).strip().lower()
    return text in {"1", "true", "t", "sim", "s", "y", "yes"}


def montar_payload_afastamentos(registros: list[dict]) -> list[dict]:
    payload = []

    for row in registros:
        cpf = row.get("cpf") or row.get("CPF") or row.get("numcpf") or row.get("cpfcol")
        if not cpf:
            continue

        descricao = str(row.get("obsafa") or row.get("sitafa") or "Afastamento")
        sigla = str(row.get("sitafa") or "AFA")[:3]

        item = {
            "cpf": str(cpf),
            "descricao": descricao,
            "sigla": sigla,
            "datainicio": _to_yyyy_mm_dd(row.get("datafa")),
            "datatermino": _to_yyyy_mm_dd(row.get("datter")),
            "rescisao": _to_bool(row.get("encafa")),
            "codigoexterno": str(row.get("seqreg") or row.get("numpro") or ""),
        }

        if item["datainicio"]:
            payload.append(item)

    return payload


def main():
    database = "SOFTRAN_COMTRASIL"
    engine = ativar_engine(database)

    repo = RepositorioAfastamentos(engine)
    afastamentos = repo.buscar_dados_afastamentos(limit=100)

    payload = montar_payload_afastamentos(afastamentos)

    print(f"Registros do banco: {len(afastamentos)}")
    print(f"Registros prontos para API: {len(payload)}")

    if not payload:
        print("Nenhum registro pronto para envio. Verifique se a consulta retorna CPF.")
        return

    print(json.dumps(payload[:5], ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
