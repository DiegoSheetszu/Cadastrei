from Ferramentas.format_cpf import format_cpf
from Ferramentas.map_genero import map_genero
from Ferramentas.to_yyyy_mm_dd import to_yyyy_mm_dd


def _text_or_default(value, default: str) -> str:
    text = str(value).strip() if value is not None else ""
    return text if text else default


def _endereco_from_row(row: dict) -> dict:
    return {
        "rua": _text_or_default(row.get("logradouro"), "NAO INFORMADO"),
        "numero": _text_or_default(row.get("numero"), "SN"),
        "complemento": "",
        "bairro": _text_or_default(row.get("bairro"), "NAO INFORMADO"),
        "cidade": _text_or_default(row.get("cidade"), "NAO INFORMADO"),
        "uf": _text_or_default(row.get("uf"), "SC"),
        "cep": _text_or_default(row.get("cep"), "00000000"),
        "latitude": 0.0,
        "longitude": 0.0,
    }


def montar_payload_motoristas(registros: list[dict]) -> list[dict]:
    payload = []

    for row in registros:
        cpf = format_cpf(row.get("numcpf"))
        nome = str(row.get("nomfun") or "").strip()
        data_admissao = to_yyyy_mm_dd(row.get("datadm"))

        if not cpf or not nome or not data_admissao:
            continue

        item = {
            "nome": nome,
            "cpf": cpf,
            "datanascimento": to_yyyy_mm_dd(row.get("datnas")),
            "genero": map_genero(row.get("tipsex")),
            "endereco": _endereco_from_row(row),
            "dataadmissao": data_admissao,
            "matricula": str(row.get("numcad") or ""),
        }

        payload.append(item)

    return payload

