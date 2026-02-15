from Ferramentas.format_cpf import format_cpf
from Ferramentas.to_bool import to_bool
from Ferramentas.to_yyyy_mm_dd import to_yyyy_mm_dd


def montar_payload_afastamentos(registros: list[dict]) -> list[dict]:
    payload = []

    for row in registros:
        cpf = format_cpf(row.get("numcpf") or row.get("cpf") or row.get("CPF") or row.get("cpfcol"))
        datainicio = to_yyyy_mm_dd(row.get("datafa"))
        descricao_situacao = str(row.get("dessit") or "").strip()

        item = {
            "numerodaempresa": row.get("numemp"),
            "tipodecolaborador": row.get("tipcol"),
            "numerodeorigemdocolaborador": row.get("numcad"),
            "cpf": cpf,
            "descricao": str(row.get("obsafa") or descricao_situacao or row.get("sitafa") or "Afastamento"),
            "descricaodasituacao": descricao_situacao or None,
            "datainicio": datainicio,
            "dataafastamento": datainicio,
            "horadoafastamento": row.get("horafa"),
            "datatermino": to_yyyy_mm_dd(row.get("datter")),
            "horadotermino": row.get("horter"),
            "situacao": row.get("sitafa"),
            "rescisao": to_bool(row.get("encafa")),
        }

        if item["datainicio"] and item["numerodeorigemdocolaborador"] is not None:
            payload.append(item)

    return payload
