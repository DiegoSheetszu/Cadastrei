from Ferramentas.format_cpf import format_cpf
from Ferramentas.to_bool import to_bool
from Ferramentas.to_yyyy_mm_dd import to_yyyy_mm_dd


def montar_payload_afastamentos(registros: list[dict]) -> list[dict]:
    payload = []

    for row in registros:
        cpf = format_cpf(row.get("numcpf") or row.get("cpf") or row.get("CPF") or row.get("cpfcol"))
        if not cpf:
            continue

        item = {
            "cpf": cpf,
            "descricao": str(row.get("obsafa") or row.get("sitafa") or "Afastamento"),
            "datainicio": to_yyyy_mm_dd(row.get("datafa")),
            "datatermino": to_yyyy_mm_dd(row.get("datter")),
            "rescisao": to_bool(row.get("encafa")),
        }

        if item["datainicio"]:
            payload.append(item)

    return payload
